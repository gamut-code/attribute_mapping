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
            
#    df = df.drop(['Grainger_Attribute_Definition', 'Grainger_Category_Specific_Definition', 'Gamut_Attribute_Definition'], axis=1) #remove unneeded columns

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
    exclusions = ['NEF', 'NPT', 'NPS', 'UNEF', 'Steel']        
    
    df['exclude'] = df['String'].apply(lambda x: ','.join([i for i in exclusions if i in x]))
#    df['exclude'] = df['String'].apply(lambda x: ','.join([i for i in exclusions if i in x]))
    
    excludeObj = df.apply(lambda x: True if x['exclude'] != "" else False , axis=1)
    exclude_count = len(excludeObj[excludeObj == True].index)
    exclude_percent = exclude_count/row_count*100

    # search for " to " in potential UOM values to detect range attributes
    range_tag = [' to ']

    df['range'] = df['String'].apply(lambda x: ','.join([i for i in range_tag if i in x]))
#    df['range'] = df['String'].apply(lambda x: ','.join([i for i in range_tag if i in x]))

    rangeObj = df.apply(lambda x: True if x['range'] != "" else False , axis=1)
    range_count = len(rangeObj[rangeObj == True].index)
    range_percent = range_count/row_count*100
        
    df.loc[df['Grainger_Attr_ID'] == attribute, '%_Numeric'] = float(percent)
    att_name = df['Grainger_Attribute_Name'].unique()
        
    if 'Thread Size' in att_name or 'Thread Depth' in att_name:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'
    elif 'Range' in att_name:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'range attribute'
    elif range_percent > 80:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'range attribute'
    elif range_percent > 0 and range_percent < 80:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'potential number; contains {} range values'.format(range_count)
    elif exclude_percent > 80:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'        
    elif percent < 80:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'text'
    elif percent >= 80 and percent < 100:           
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'potential number'
    elif percent == 100:
        df.loc[df['Grainger_Attr_ID'] == attribute, 'Data Type'] = 'number'
        
    df['%_Numeric'] = df['%_Numeric'].map('{:,.2f}'.format)

    return df


def match_lovs(df, lov_list, attribute):
    """compare the 'Grainger_Attr_ID' column against our list of LOVs"""
    attr_id = str(attribute) + '_ATTR'
    
    if attr_id in lov_list:
        df['Restricted Attribute Value Domain'] = 'Y'

    return df
  
    
def determine_uoms(df, uom_df):
    """for all non 'text' data types, compare 'String' field to our UOM list, then compare these potential UOMs
    to current GWS UOM groupings. finally, determine whether numeric part of the value is a fraction or decimal"""

    unit_df = pd.DataFrame()
    potential_list = list()
    uom_list = list()
    
    # build unique UOM list for comparison
    uom_list = uom_df['unit_name'].tolist()
    uom_list = set(uom_list)
    print(uom_list)

    for row in df.itertuples():
        # for non text fields, run a search for potential UOM groups and categorize
        val = df.at[row.Index,'Data Type']

        if val != 'text':
            text_value = df.at[row.Index,'String']
            text_value = str(text_value)

            # if 'String' field contains value(s), compare to UOM list and assigned to 'Potential UOMs'
            if text_value != '':
                pot_uom = [x for x in uom_list if x in text_value.split()]
                df.at[row.Index,'Potential UOMs'] = pot_uom

                # create list all unique potential UOMs for the attribute
                if pot_uom:
                    if pot_uom not in potential_list:
                        potential_list.append(pot_uom)

        # evaluate whether 'Numeric' value can be classified as decimal or fraction
        num = df.at[row.Index,'Numeric']
        
        if '.' in str(num):
            df.at[row.Index,'Numeric display type'] = 'decimal'
        elif '/' in str(num):
            df.at[row.Index,'Numeric display type'] = 'fraction'

    for unit in potential_list:
        print ('unit = ', unit)
        temp_uom = uom_df.loc[uom_df['unit_name']== unit]
        unit_df = pd.concat([unit_df, temp_uom], axis=0, sort=False)

        if unit_df.empty == False:
            df['Unit of Measure Domain'] = '; '.join(item for item in str(unit_df['unit_group_id']) if item)
            df['Unit of Measure Group Name'] = '; '.join(item for item in str(unit_df['unit_group_name']) if item)

    return df

    
def analyze(df, uom_df, lov_list):
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
    
    for attribute in atts:
        temp_df = df.loc[df['Grainger_Attr_ID']== attribute]

        temp_df = get_data_type(temp_df, attribute)
        temp_df = match_lovs(temp_df, lov_list, attribute)
        temp_df = determine_uoms(temp_df, uom_df)

        analyze_df = pd.concat([analyze_df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
        
    analyze_df.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\moist.csv')

    return analyze_df


def grainger_process(grainger_df, grainger_all, uom_df, lov_list, gamut_dict: Dict, k):
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
    
    grainger_df = analyze(grainger_df, uom_df, lov_list)

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
 
                skus = gamut_skus[gamut_skus['Gamut_Node_ID'] == node]
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 
   #             temp_df['grainger_sku_count'] = grainger_sku_count
   #             temp_df['gamut_sku_count'] = len(skus)
   #             temp_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '+node_name
   #             temp_df['Gamut/Grainger SKU Counts'] = temp_df['gamut_sku_count'].map(str)+' / '+temp_df['grainger_sku_count'].map(str)

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')
                # drop all of the rows that are 'Gamut only' in the Match column
                df = df[df.Matching != 'Gamut only']

            else:
                print('GWS Node {} EMPTY DATAFRAME'.format(node))
    else:
    #    grainger_df['Gamut/Grainger SKU Covunts'] = '0 / '+str(grainger_sku_count)
    #    grainger_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '
        df = grainger_df
        print('No Gamut SKUs for Grainger node {}'.format(k))

    df.reset_index(drop=True, inplace=True)
    df = choose_definition(df)

    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes


def attribute_process(grainger_df, uom_df, lov_list, node):
    attribute_df = pd.DataFrame()
    grainger_att_vals = pd.DataFrame()
    gamut_dict = dict()

    grainger_att_vals = q.grainger_values(grainger_df)

    temp_df, gamut_dict = grainger_process(grainger_df, grainger_att_vals, uom_df, lov_list, gamut_dict, k)
    attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
    print ('Grainger node = ', node)
    
    attribute_df = attribute_df.drop(['Count', 'alt_grainger_name', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path'], axis=1)        
        
    attribute_df = attribute_df.rename(columns={'Segment_ID':'Segment ID', 'Segment_Name':'Segment Name', \
                'Family_ID':'Family ID', 'Family_Name':'Family Name', 'Category_ID':'Category ID', \
                'Category_Name':'Category Name', 'Grainger_Attr_ID':'Attribute_ID', \
                'Grainger_Attribute_Name':'Attribute Name'})

    return attribute_df


def build_df(data_type, search_data, uom_df, lov_list):
    """this is the core set of instructions that builds the dataframes for export"""
    grainger_df = pd.DataFrame()

    start_time = time.time()
    print('working...')

    if data_type == 'grainger_query':
        if search_level == 'cat.CATEGORY_ID':
            for k in search_data:
                grainger_df = q.gcom.grainger_q(grainger_attr_query, search_level, k)

                if grainger_df.empty == False:
                    df_upload = attribute_process(grainger_df, uom_df, lov_list, k)
                else:
                    print('No attribute data')
        else:
            for k in search_data:
                print('K = ', k)
                grainger_skus = q.grainger_nodes(k, search_level)
                grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
                print('grainger L3s = ', grainger_l3)

                for j in grainger_l3:
                    grainger_df = q.gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', j)

                    if grainger_df.empty == False:
                        df_upload = attribute_process(grainger_df, uom_df, lov_list, j)
                    else:
                        print('No attribute data')
                print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
        
    return df_upload


#determine SKU or node search
search_level = 'cat.CATEGORY_ID'

# read in uom and LOV files
uom_df = pd.DataFrame()

uom_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_data_sheet.csv'

# create df of the uom groupings (ID and UOMs for each group)
data_file = requests.get(uom_groups_url).content
uom_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

# read in the LOV list
lov_list = q.lov_values()

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level, data_process = fd.blue_search_level()

    if data_process == 'one':
        file_data = settings.get_files_in_directory()
        for file in file_data:
            search_data = [int(row[0]) for row in file_data[file][1:]]
            df_upload =  build_df(data_type, search_data, uom_df, lov_list)
            fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
            
    elif data_process == "two":
        search_data = fd.data_in(data_type, settings.directory_name)

        for k in search_data:
            df_upload =  build_df(data_type, search_data, uom_df, lov_list)
            
            if df_upload.empty==False:
                fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
            else:
                print('EMPTY DATAFRAME')