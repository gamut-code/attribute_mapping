# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import csv
import re
from grainger_query import GraingerQuery
from queries_MATCH import gamut_attr_query, grainger_attr_query, grainger_value_query
import data_process as process
import query_code as q
import file_data_att as fd
import settings
import time

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
#    gamut_sample_vals = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()

    gamut_df = q.gamut_atts(gamut_attr_query, node, 'tax.id')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\

    if gamut_df.empty==False:
#        gamut_att_vals, gamut_sample_vals = q.gamut_values(gamut_attr_values, node, 'tax.id') #gamut_values exports a list of --all-- normalized values and sample_values
         
#        if gamut_att_vals.empty==False:
#            gamut_sample_vals = gamut_sample_vals.rename(columns={'Normalized Value': 'Gamut Attribute Sample Values'})
#            gamut_df = pd.merge(gamut_df, gamut_sample_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
#            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attr_ID'])  #add t0p 5 normalized values to report

        gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
        
        gamut_dict[node] = gamut_df #store the processed df in dict for future reference 
    else:
        print('{} EMPTY DATAFRAME'.format(node))    
        
    return gamut_dict, gamut_df


def grainger_assign_nodes (grainger_df, gamut_df, node):
    """assign gamut node data to grainger columns"""
    
    att_list = []
    
    node_ID = gamut_df['Gamut_Node_ID'].unique()
    cat_ID = gamut_df['Gamut_Category_ID'].unique()
    cat_name = gamut_df['Gamut_Category_Name'].unique()
    node_name = gamut_df['Gamut_Node_Name'].unique()
    pim_path = gamut_df['Gamut_PIM_Path'].unique()

    atts = grainger_df['Grainger_Attribute_Name'].unique()
    att_list = [att for att in atts if att]
    att_list = np.char.strip(att_list)

    for att in att_list:
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Node_ID'] = node_ID
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Category_ID'] = cat_ID
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Category_Name'] = cat_name
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Node_Name'] = node_name
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_PIM_Path'] = pim_path
    
    return grainger_df


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
            value = row.Grainger_Attribute_Value
            
            r = re.compile('^\d*[\.\/]?\d*')

            temp_df.at[row.Index, 'Numeric'], temp_df.at[row.Index, 'String'] = re.split(r, value)
            num = r.search(value)           
            temp_df.at[row.Index, 'Numeric'] = num.group()

        all_vals = pd.concat([all_vals, temp_df], axis=0)

    return all_vals


def detect_UOMs(sum):
    """read in the latest copy of "UOM_data_sheet.csv" and compare against the 'String' column for potential UOMs"""

    with open('UOM_data_sheet.csv', newline='') as f:
    reader = csv.reader(f)
    data = list(reader)

def analyze(df):
    """use the split fields in grainger_df to analyze suitability for number conversion and included in summary df"""

    sum = pd.DataFrame()
    
    # create the numeric/string columns
    grainger_df = split(grainger_df)

    atts = df['Grainger_Attribute_Name'].unique()

    sum['%_Numeric'] = ''
    sum['Candidate'] = ''
    sum['Potential UOMs'] = ''
    
    for attribute in atts:
        temp_att = df.loc[df['Grainger_Attribute_Name']== attribute]

        row_count = len(temp_att.index)

        # Get a bool series representing positive 'Num' rows
        seriesObj = temp_att.apply(lambda x: True if x['Numeric'] != "" else False , axis=1) 
        # Count number of True in series
        num_count = len(seriesObj[seriesObj == True].index)
        percent = num_count/row_count*100

        # build a list of items that are exluded as potential UOM values
        # if found, put values in a separate column used for evaluating 'Candidate' below
        exclusions = ['NEF', 'NPT', 'NPS', 'UNEF', 'Steel']        
        temp_att['exclude'] = temp_att['String'].apply(lambda x: ','.join([i for i in exclusions if i in x]))
        excludeObj = temp_att.apply(lambda x: True if x['exclude'] != "" else False , axis=1)
        exclude_count = len(excludeObj[excludeObj == True].index)
        exclude_percent = exclude_count/row_count*100

        detect_UOMs(sum)
        
        # search for " to " in potential UOM values to detect range attributes
        rangeObj = temp_att.apply(lambda x: True if x['Potential UOMs'] in ' to ' else False , axis=1) 
        range_count = len(rangeObj[rangeObj == True].index)
        
        print('exclude count = ', exclude_count)
        print('row count     = ', row_count)
        print('exclude percent ', exclude_percent)
        sum.loc[sum['Grainger_Attribute_Name'] == attribute, '%_Numeric'] = float(percent)
        
        if 'Thread Size' in attribute or 'Thread Depth' in attribute:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'
        if 'Range' in attribute or range_count > 0:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'range'
        elif exclude_percent > 80:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'        
        elif percent < 80:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'
        elif percent >= 80 and percent < 100:           
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'potential'
        elif percent == 100:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'Y'
        
    sum['%_Numeric'] = sum['%_Numeric'].map('{:,.2f}'.format)

    return sum


def grainger_process(grainger_df, grainger_sample, grainger_all, fill_rate, gamut_dict: Dict, k):
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
    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df['Grainger Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']

    grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns
    
    grainger_df = pd.merge(grainger_df, grainger_sample, on=['Grainger_Attribute_Name'])
    grainger_df = pd.merge(grainger_df, grainger_all, on=['Grainger_Attr_ID'])
    grainger_df = pd.merge(grainger_df, fill_rate, on=['Grainger_Attribute_Name'])
    
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
                grainger_df = grainger_assign_nodes(grainger_df, gamut_df, node)
                gamut_df = gamut_assign_nodes(grainger_df, gamut_df)
 
                skus = gamut_skus[gamut_skus['Gamut_Node_ID'] == node]
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 
   #             temp_df['grainger_sku_count'] = grainger_sku_count
   #             temp_df['gamut_sku_count'] = len(skus)
   #             temp_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '+node_name
   #             temp_df['Gamut/Grainger SKU Counts'] = temp_df['gamut_sku_count'].map(str)+' / '+temp_df['grainger_sku_count'].map(str)

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')
                df = choose_definition(df)

            else:
                print('GWS Node {} EMPTY DATAFRAME'.format(node))
    else:
    #    grainger_df['Gamut/Grainger SKU Counts'] = '0 / '+str(grainger_sku_count)
    #    grainger_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '
        df = grainger_df
        print('No Gamut SKUs for Grainger node {}'.format(k))
        
    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes


def attribute_process_singular(data_type, search_data):
    gamut_df = pd.DataFrame()
    grainger_df = pd.DataFrame()
    grainger_skus = pd.DataFrame()

    df_upload = pd.DataFrame()
    grainger_att_vals = pd.DataFrame()
    grainger_sample_vals = pd.DataFrame()
    grainger_fill_rates = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()
    gamut_dict = dict()

    start_time = time.time()
    print('working...')

    if data_type == 'grainger_query':
        if search_level == 'cat.CATEGORY_ID':
            for k in search_data:
                grainger_df = q.gcom.grainger_q(grainger_attr_query, search_level, k)

                if grainger_df.empty == False:
                    grainger_att_vals, grainger_sample_vals, grainger_fill_rates = q.grainger_values(grainger_df)
                    grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Gamut Attribute Sample Values'})
                    grainger_att_vals = grainger_att_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger ALL Values'})

                    temp_df, gamut_dict = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, grainger_fill_rates, gamut_dict, k)
                    df_upload = pd.concat([df_upload, temp_df], axis=0, sort=False)
                    print ('Grainger node = ', k)
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
                        grainger_att_vals, grainger_sample_vals, grainger_fill_rates = q.grainger_values(grainger_df)
                        grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
                        temp_df, gamut_dict = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, grainger_fill_rates, gamut_dict, j)
                        df_upload = pd.concat([df_upload, temp_df], axis=0, sort=False)
                        print ('Grainger ', j)
                    else:
                        print('Grainger node {} All SKUs are R4, R9, or discontinued'.format(j)) 
                print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
        
    elif data_type == 'sku':
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
        grainger_df = q.gcom.grainger_q(grainger_attr_query, 'item.MATERIAL_NO', sku_str)
        if grainger_df.empty == False:
            grainger_att_vals, grainger_sample_vals = q.grainger_values(grainger_df)
            grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
            temp_df, gamut_dict = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, gamut_dict, sku_str)
            df_upload = pd.concat([df_upload, temp_df], axis=0, sort=False)
        else:
            print('All SKUs are R4, R9, or discontinued') 
            
    print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

    return df_upload


def build_df(data_type, search_data):
    """this is the core set of instructions that builds the dataframes for export"""
 #   df_upload = pd.DataFrame()
 #   df_summary = pd.DataFrame()
 #   grainger_df = pd.DataFrame()
    
    grainger_df = attribute_process_singular(data_type, search_data)
    grainger_df = grainger_df.drop(['Count'], axis=1)
    
    df_upload = grainger_df
    
    grainger_df = split(grainger_df)
    df_summary = analyze(grainger_df)

    return df_upload, df_summary


#determine SKU or node search
search_level = 'cat.CATEGORY_ID'


data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level, data_process = fd.blue_search_level()

    if data_process == 'one':
        file_data = settings.get_files_in_directory()
        for file in file_data:
            search_data = [int(row[0]) for row in file_data[file][1:]]
            df_upload, df_summary =  build_df(data_type, search_data)
            fd.GWS_upload_data_out(settings.directory_name, df_upload, df_summary, search_level)
            
    elif data_process == "two":
        search_data = fd.data_in(data_type, settings.directory_name)

        for k in search_data:
            df_upload, df_summary =  build_df(data_type, search_data)
            
            if grainger_df.empty==False:
                fd.GWS_upload_data_out(settings.directory_name, df_upload, df_summary, search_level)
            else:
                print('EMPTY DATAFRAME')