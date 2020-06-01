# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import requests
import io
import re
from grainger_query import GraingerQuery
from queries_NUMERIC import gamut_attr_query, grainger_attr_query, gamut_attr_values
import data_process as process
import query_code_NUMERIC as q
import file_data_att as fd
from typing import Dict
import settings
import time

pd.options.mode.chained_assignment = None

gcom = GraingerQuery()


def match_category(df):
    """compare data colected from matching file (match_df) with grainger and gamut data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""
    
    df['Matching'] = 'no'
    
    for row in df.itertuples():
        grainger_string = str(row.Grainger_Attribute_Name)
        gamut_string = str(row.Gamut_Attribute_Name)
        
        if (grainger_string) == (gamut_string):
            df.at[row.Index,'Matching'] = 'Match'
        elif (grainger_string) in (gamut_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif (gamut_string) in (grainger_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif process.isBlank(row.Grainger_Attribute_Name) == False:
            if process.isBlank(row.Gamut_Attribute_Name) == True:
                df.at[row.Index,'Matching'] = 'Grainger only'
        elif process.isBlank(row.Grainger_Attribute_Name) == True:
            if process.isBlank(row.Gamut_Attribute_Name) == False:
                df.at[row.Index,'Matching'] = 'Gamut only'
            
    return df


def choose_definition(df):
    """pick the definition to upload to GWS based on 1. Cat specific, 2. Attribute level, 3. old Gamut definition"""
    
    df['Definition'] = ''
    
    for row in df.itertuples():
        cat_def = str(row.Grainger_Category_Specific_Definition)
        attr_def = str(row.Grainger_Attribute_Definition)
        gamut_def = str(row.Gamut_Attribute_Definition)
        
        if process.isBlank(row.Grainger_Category_Specific_Definition) == False:
            df.at[row.Index,'Definition'] = cat_def
        elif process.isBlank(row.Grainger_Attribute_Definition) == False:
            df.at[row.Index,'Definition'] = attr_def
        elif process.isBlank(row.Gamut_Attribute_Definition) == False:
            df.at[row.Index,'Definition'] = gamut_def
            
    if df.empty == False:
        df = df.drop(['Grainger_Attribute_Definition', 'Grainger_Category_Specific_Definition', 'Gamut_Attribute_Definition'], axis=1) #remove unneeded columns

    return df


def gamut_process(node, gamut_dict: Dict, k):
    """if gamut node has not been previously processed (in gamut_dict), process and add it to the dictionary"""

    gamut_df = q.gamut_atts(gamut_attr_query, node, 'tax.id')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\

    if gamut_df.empty==False:
        gamut_att_vals = q.gamut_values(gamut_attr_values, node, 'tax.id') #gamut_values exports a list of --all-- normalized values and sample_values

        if gamut_att_vals.empty==False:
            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attr_ID'])  #add top 5 normalized values to report

        gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
        
        gamut_dict[node] = gamut_df #store the processed df in dict for future reference 
    else:
        print('{} EMPTY DATAFRAME'.format(node))    
        
    return gamut_dict, gamut_df


def split(df):
    """ split values into numerators + UOMs and create separate columns for each"""
    all_vals = pd.DataFrame()
    atts = df['Grainger_Attribute_Name'].unique()

    for attribute in atts:
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df['Numeric'] = ""
        temp_df['String'] = ""

        for row in temp_df.itertuples():
            value = str(row.Grainger_Attribute_Value)

            r = re.compile('^\d*[\.\/]?\d*')

            temp_df.at[row.Index, 'Numeric'], temp_df.at[row.Index, 'String'] = re.split(r, value)
            num = r.search(value)           
            temp_df.at[row.Index, 'Numeric'] = num.group()

            temp_df['String'] = temp_df['String'].str.strip()

        all_vals = pd.concat([all_vals, temp_df], axis=0)

    return all_vals

    
def get_data_type(df, attribute):
    """using 'Numeric' and 'String' column values, determine which attributes are recommended as numeric, text, or range"""
    row_count = len(df.index)

    # Get a bool series representing positive 'Num' rows
    seriesObj = df.apply(lambda x: True if x['Numeric'] != "" else False , axis=1) 
    # Count number of True in series
    num_count = len(seriesObj[seriesObj == True].index)
    percent = num_count/row_count*100

    # build a list of items that are exluded as potential UOM values
    # if found, put values in a separate column used for evaluating 'Candidate' below
    value_exclusions = ['NEF', 'NPT', 'NPS', 'UNEF', 'Steel']        
    
    df['exclude'] = df['String'].apply(lambda x: ','.join([i for i in value_exclusions if i in x]))    
    excludeObj = df.apply(lambda x: True if x['exclude'] != "" else False , axis=1)
    exclude_count = len(excludeObj[excludeObj == True].index)
    exclude_percent = exclude_count/row_count*100

    # search for " to " in potential UOM values to detect range attributes
    range_tag = [' to ', ' x ']
    df['range'] = df['String'].apply(lambda x: ','.join([i for i in range_tag if i in x]))
    rangeObj = df.apply(lambda x: True if x['range'] != "" else False , axis=1)
    range_count = len(rangeObj[rangeObj == True].index)
    range_percent = range_count/row_count*100
        
    df.loc[df['Grainger_Attr_ID'] == attribute, '%_Numeric'] = float(percent)
 
    # build a list of attributes that should automatically be considered "text"
    att_name = df['Grainger_Attribute_Name'].unique()
    att_name = att_name[0]

    evaluated = 'n'
    
    name_exclusions = ['Thread Size', 'Thread Depth', 'Item', 'For Use With', 'Connection', 'Material', 'Type']
#    exc = any(att_name in x for x in name_exclusions)
    for name in name_exclusions:
        if name in att_name:
            evaluated = 'y'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'

    if evaluated == 'n':
        if 'Range' in att_name:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'multi-valued attribute'
        elif exclude_percent > 80:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'        
        elif percent < 80:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'
        elif range_percent > 70:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'multi-valued attribute'
        elif percent >= 80 and percent < 100:           
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'potential number'
        elif percent == 100:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'number'
        elif range_percent > 0 and range_percent < 70:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'potential number; contains {} multi-valued attributes'.format(range_count)

    df['%_Numeric'] = df['%_Numeric'].map('{:,.2f}'.format)

    return df


def match_lovs(lov_df, attribute):
    """compare the 'Grainger_Attr_ID' column against our list of LOVs"""
    
    lov_list = list()
    values_list = list()
    
    attr_id = str(attribute) + '_ATTR'

    lov_list = lov_df['AttributeID'].tolist()
    lov_list = set(lov_list)
        
    if attr_id in lov_list:
        temp_df = lov_df.loc[lov_df['AttributeID']== attr_id]
        values_list = temp_df['Values'].tolist()    

    return values_list
  
    
def determine_uoms(df, uom_df, values_list):
    """for all non 'text' data types, compare 'String' field to our UOM list, then compare these potential UOMs
    to current GWS UOM groupings. finally, determine whether numeric part of the value is a fraction or decimal"""

    unit_df = pd.DataFrame()
    text_list = list()
    text_list_lower = list()
    name_list = list()
    name_list_lower = list()
    potential_list = list()
    best_potential_list = list()
    second_pot_list = list()
    match_name_list = list()
    second_pot_name_list = list()
    uom_list = list()
    uom_ids =  list()
    uom_names = list()
    matched_ids = list()
    intersect_list = list()
    unit_names = list()
    uom_dict = dict()
    final_list = list()

    # build unique UOM list for comparison
    uom_list = uom_df['unit_name'].tolist()
    uom_list = set(uom_list)

    for row in df.itertuples():
        # for non text fields, run a search for potential UOM groups and categorize
        data_type = df.at[row.Index,'Data Type']

        if data_type != 'text':
            # evaluate the text portion of attribute value field
            str_value = df.at[row.Index,'String']
            str_value = str(str_value)
            str_value = str_value.replace('to',' ')
            str_value = str_value.replace('-',' ')

            if ' x ' in str_value:
                str_value = str_value.replace(' x ',' ')
                str_value = str_value.replace(' L', ' ')
                str_value = str_value.replace(' H', ' ')
                str_value = str_value.replace(' W', ' ')
            str_value_lower = str_value.lower()
            
            # force 'String' content into a list so we can evaulate the entire string for a match against uom_list
            text_list.append(str_value)
            text_list_lower.append(str_value_lower)
            
            # if 'String' field contains value(s), compare to UOM list and assigned to 'Potential UOMs'
            if str_value != '':
                # check for a match of the entire contant of 'String' against our uom_list
                match = set(text_list).intersection(set(uom_list))
                # but if we don't find an exact match, parse 'String' content and attempt to match up with uom_List

                if not match:
                    match = set(text_list_lower).intersection(set(uom_list))
                    
                if not match:
                    pot_uom = [x for x in uom_list if x in str_value.split()]
                    # if parse by word match still fails, try one more time at a more granular level for a match                    

                    if not pot_uom:
                        pot_uom = [x for x in uom_list if x in str_value_lower.split()]
                        
                # create list of potential UOMs for the attribute
                if match:
                    best_potential_list.extend(match)
                elif pot_uom:
                    second_pot_list.extend(pot_uom)

            # consider attribute name field as a sourse of potential uoms also
            # evalulate lower case versions of attribute names also, to look for matches like "PSI"
            name_value = df.at[row.Index, 'Grainger_Attribute_Name']
            name_value = str(name_value)
            name_value = name_value.replace('to','')
            
            name_value_lower = name_value.lower()

            name_list.append(name_value)
            name_list_lower.append(name_value_lower)

            name_match = set(name_list).intersection(set(uom_list))
            
            if not name_match:
                name_match = set(name_list_lower).intersection(set(uom_list))

            if not name_match:
                pot_name_uom = [x for x in uom_list if x in name_value.split()]

                if not pot_name_uom:
                    pot_name_uom = [x for x in uom_list if x in name_value_lower.split()]

            # create list of potential UOMs for the attribute
            if name_match:
                match_name_list.extend(name_match)
            elif pot_name_uom:
                second_pot_name_list.extend(pot_name_uom)

        # evaluate whether 'Numeric' value can be classified as decimal or fraction
        num = df.at[row.Index,'Numeric']
        num = str(num)
        
        if data_type != 'text':
            if num != '':
                if '.' in num:
                    df.at[row.Index,'Numeric display type'] = 'decimal'
                elif '/' in num:
                    df.at[row.Index,'Numeric display type'] = 'fraction'
                else:
                    df.at[row.Index,'Numeric display type'] = 'decimal'
                
    if best_potential_list:
        potential_list = set(best_potential_list)
    elif second_pot_list:
        potential_list = set(second_pot_list)
    elif match_name_list:
        potential_list = set(match_name_list)
    elif second_pot_name_list:
        potential_list = set(second_pot_name_list)
            
    for unit in potential_list:
        if len(potential_list) > 1:
            temp_df = uom_df.loc[uom_df['unit_name']== unit]
  #          temp_df['%_UOM_Match'] = ''
            
            # create a pool of all ids that contain the specific UOM
            matched_ids = temp_df['unit_group_id'].tolist()                
            matched_ids = [int(x) for x in matched_ids if ~np.isnan(x)]

            for match in matched_ids:
                temp_uom =   uom_df.loc[uom_df['unit_group_id']== match]
                unit_names = temp_uom['unit_name'].tolist()
                intersect_list = set(potential_list).intersection(set(unit_names))
                match_percent = len(intersect_list)/len(potential_list)*100
                match_percent = round(match_percent, 2)
                # create dictionary entry for each matched uom + match percentage 
                
                if match not in uom_dict:
                    uom_dict[match] = match_percent
                        
            temp_df = temp_df[['unit_group_id', 'unit_group_name', 'unit_name']]
            unit_df = pd.concat([unit_df, temp_df], axis=0)

        else:
            unit_df = uom_df.loc[uom_df['unit_name'] == unit]

    df = df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
            
    if unit_df.empty == False:
        unit_df = unit_df.drop_duplicates(subset=['unit_group_id'])  #group by Category_ID and attribute name and keep unique

        dict_display = ''
        
        if uom_dict:
            final_list = [[k,v] for k, v in uom_dict.items()]
            # sort the output by highest matching percentages
            final_list = sorted(final_list,key = lambda l:l[1], reverse=True)
            
            dict_display = '  '.join([str(value) for value in final_list])
            dict_display = dict_display.replace(",", ":")

        # create list of the sorted uom_ids from final list to use in sorting the df
        sorter = [i[0] for i in final_list]
        # create dictionary that defines the order for sorting
        sorterIndex = dict(zip(sorter,range(len(sorter))))    
        unit_df['id_rank'] = unit_df['unit_group_id'].map(sorterIndex)
        unit_df.sort_values(['id_rank'], ascending = [True], inplace = True)

        uom_ids = unit_df['unit_group_id'].tolist()
        uom_ids = [int(x) for x in uom_ids if ~np.isnan(x)]

        uom_names = unit_df['unit_group_name'].tolist()

    for row in df.itertuples():
        # if LOV and/or UOM lists are populated, write them to the df
        if values_list:
            df.at[row.Index, 'Restricted Attribute Value Domain'] = values_list

        if potential_list:
            df.at[row.Index, 'Potential UOMs'] = potential_list

        if uom_ids:
            if len(uom_ids) == 1:
                single_id = uom_ids.pop()
                df.at[row.Index,'Unit of Measure Domain'] = single_id
            elif dict_display != '':
                df.at[row.Index,'Unit of Measure Domain'] = dict_display
            else:
                df.at[row.Index,'Unit of Measure Domain'] = uom_ids
                
            df.at[row.Index,'Unit of Measure Group Name'] = uom_names

    return df

    
def analyze(df, uom_df, lov_df):
    """use the split fields in grainger_df to analyze suitability for number conversion and included in summary df"""
    analyze_df = pd.DataFrame()
    
    # create the numeric/string columns
    df = split(df)

    atts = df['Grainger_Attr_ID'].unique()

    df['%_Numeric'] = ''
    df['Data Type'] = ''
    df['Potential UOMs'] = ''
    df['Unit of Measure Domain'] = ''
    df['Unit of Measure Group Name'] = ''
    df['Restricted Attribute Value Domain'] = 'N'
    df['Numeric display type'] = ''
 #   df['%_UOM_Match'] = ''
    
    for attribute in atts:
        temp_df = df.loc[df['Grainger_Attr_ID']== attribute]
        temp_df = get_data_type(temp_df, attribute)
        values_list = match_lovs(lov_df, attribute)
        temp_df = determine_uoms(temp_df, uom_df, values_list)

        analyze_df = pd.concat([analyze_df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
        
#    analyze_df.to_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test.csv')
    return analyze_df


def grainger_process(grainger_df, grainger_all, uom_df, lov_df, gamut_dict: Dict, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    
    df = pd.DataFrame()
        
    cat_name = grainger_df['Category_Name'].unique()
    cat_name = list(cat_name)
    cat_name = cat_name.pop()
    print('cat name = {} {}'.format(k, cat_name))

    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gamut query
    grainger_sku_count = len(grainger_skus)
    print('grainger sku count = ', grainger_sku_count)
    
    grainger_df = analyze(grainger_df, uom_df, lov_df)

    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df['STEP Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']

    grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns    
    grainger_df = pd.merge(grainger_df, grainger_all, on=['Grainger_Attr_ID'])    
    grainger_df['alt_grainger_name'] = process.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge

    gamut_skus = q.gamut_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull
    
    if gamut_skus.empty==False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node 
        gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        print('GWS L3s ', gamut_l3)
        
        for node in gamut_l3:
            if node in gamut_dict:
                gamut_df = gamut_dict[node]
                print ('node {} in gamut dict'.format(node))
            else:
                gamut_dict, gamut_df = gamut_process(node, gamut_dict, k)

            if gamut_df.empty==False:
                node_name = gamut_df['Gamut_Node_Name'].unique()
                node_name = list(node_name)
                node_name = node_name.pop()
                print('node name = {} {}'.format(node, node_name))
                #add correlating grainger and gamut data to opposite dataframes
                grainger_df = q.grainger_assign_nodes(grainger_df, gamut_df, node)
                gamut_df = q.gamut_assign_nodes(grainger_df, gamut_df)
 
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')

                # drop all of the rows that are 'Gamut only' in the Match column
                df = df[df.Matching != 'Gamut only']
                
                df = df.drop(['Count', 'alt_grainger_name', 'Gamut_Node_ID', 'Gamut_Category_ID', 'Gamut_Category_Name', \
                              'Gamut_Node_Name', 'Gamut_PIM_Path'], axis=1)        

            else:
                print('GWS Node {} EMPTY DATAFRAME'.format(node))
    else:
        df = grainger_df
        df['Gamut_Attribute_Definition'] = ''
        print('No Gamut SKUs for Grainger node {}'.format(k))

    df.reset_index(drop=True, inplace=True)
    df = choose_definition(df)

    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes


def attribute_process(grainger_df, uom_df, lov_df, node):
    attribute_df = pd.DataFrame()
    grainger_att_vals = pd.DataFrame()
    gamut_dict = dict()

    grainger_att_vals = q.grainger_values(grainger_df)

    temp_df, gamut_dict = grainger_process(grainger_df, grainger_att_vals, uom_df, lov_df, gamut_dict, node)
    attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
    print ('Grainger node = ', node)

    attribute_df = attribute_df.drop_duplicates(subset=['Grainger_Attr_ID'])
            
    attribute_df = attribute_df.rename(columns={'Segment_ID':'Segment ID', 'Segment_Name':'Segment Name', \
                'Family_ID':'Family ID', 'Family_Name':'Family Name', 'Category_ID':'Category ID', \
                'Category_Name':'Category Name', 'Grainger_Attr_ID':'Attribute_ID', \
                'Grainger_Attribute_Name':'Attribute Name', 'Gamut Sample Values':'Gamut Attribute Sample Values'})

    attribute_df['Multivalued?'] = 'Y'
    
    return attribute_df


def build_df(data_process, data_type, search_data, uom_df, lov_df):
    """this is the core set of instructions that builds the dataframes for export"""
    grainger_df = pd.DataFrame()
    df_upload = pd.DataFrame()
    
    start_time = time.time()

    if data_type == 'grainger_query':
        if search_level == 'cat.CATEGORY_ID':
            for k in search_data:
                grainger_df = q.gcom.grainger_q(grainger_attr_query, search_level, k)

                if grainger_df.empty == False:
                    df_upload = attribute_process(grainger_df, uom_df, lov_df, k)
                else:
                    print('No attribute data')
                    
                if df_upload.empty==False:
                    fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
                else:
                    print('EMPTY DATAFRAME')
                    
                print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

        else:
            for k in search_data:
                print('K = ', k)
                grainger_skus = q.grainger_nodes(k, search_level)
                grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
                print('grainger L3s = ', grainger_l3)

                for j in grainger_l3:
                    grainger_df = q.gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', j)

                    if grainger_df.empty == False:
                        temp_df = attribute_process(grainger_df, uom_df, lov_df, j)
                        df_upload = pd.concat([df_upload, temp_df], axis=0, sort=False)

                    else:
                        print('No attribute data')
                
                if df_upload.empty==False:
                    fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
                else:
                    print('EMPTY DATAFRAME')                   

                print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

        
    return df_upload


#determine SKU or node search
search_level = 'cat.CATEGORY_ID'

# read in uom and LOV files
uom_df = pd.DataFrame()

uom_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_data_sheet.csv'
lov_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/LOV_list.csv'

# create df of the uom groupings (ID and UOMs for each group)
data_file = requests.get(uom_groups_url).content
uom_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

# create df of the lovs and their concat values
data_file = requests.get(lov_groups_url).content
lov_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

data_type = fd.search_type()

print('working...')

if data_type == 'grainger_query':
    search_level, data_process = fd.blue_search_level()

    if data_process == 'one':
        file_data = settings.get_files_in_directory()
        for file in file_data:
            search_data = [int(row[0]) for row in file_data[file][1:]]
            df_upload =  build_df(data_process, data_type, search_data, uom_df, lov_df)
            fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
            
    elif data_process == "two":
        search_data = fd.data_in(data_type, settings.directory_name)
        df_upload =  build_df(data_process, data_type, search_data, uom_df, lov_df)
