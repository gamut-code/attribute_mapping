# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 16:39:52 2019

@author: xcxg109
"""

import pandas as pd
import query_code as q
import data_pull as pull
import data_process as process
import file_data_att as fd
import settings
from queries_PIM import grainger_attr_query
import time


pd.options.mode.chained_assignment = None


def match_category(df):
    """compare data colected from matching file (match_df) with grainger and gamut data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""

    for row in df.itertuples():
        if (row.Index, row.Grainger_Attribute_Name) == (row.Index, row.Gamut_Attribute_Name):
            df.at[row.Index,'Matching'] = 'Match'
        elif process.isBlank(row.Grainger_Attribute_Name) == False:
            if process.isBlank(row.Gamut_Attribute_Name) == True:
                df.at[row.Index,'Matching'] = 'Grainger only'
        elif process.isBlank(row.Grainger_Attribute_Name) == True:
            if process.isBlank(row.Gamut_Attribute_Name) == False:
                df.at[row.Index,'Matching'] = 'Gamut only'
             
    return df


def grainger_process(grainger_df, grainger_sample, grainger_all, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    
    df = pd.DataFrame()
    gamut_sample_vals = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()
  #  gamut_l3 = dict()
    
    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gamut query
    grainger_sku_count = len(grainger_skus)
    print('Grainger SKU count = ', grainger_sku_count)

#    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df['Grainger Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']
 #   grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns
    grainger_df = pd.merge(grainger_df, grainger_sample, on=['Grainger_Attribute_Name'])
    grainger_df = pd.merge(grainger_df, grainger_all, on=['Grainger_Attribute_Name'])
    
    grainger_df['Grainger_Attribute_Name'] = process.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge
    grainger_df.to_csv ("F:/CGabriel/Grainger_Shorties/OUTPUT/grainger_test.csv")
    
    gamut_skus = q.gamut_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull
    gamut_skus = gamut_skus.drop_duplicates(subset='Gamut_SKU')

    if gamut_skus.empty == False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node
        gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        for node in gamut_l3:
            gamut_df = q.gamut_atts(node, 'tax.id')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node
            gamut_att_vals, gamut_sample_vals = q.gamut_values(gamut_df) #gamut_values exports a list of --all-- normalized values (temp_df) and sample_values
            gamut_sample_vals = gamut_sample_vals.rename(columns={'Normalized Value': 'Gamut Attribute Sample Values'})
            gamut_att_vals = gamut_att_vals.rename(columns={'Normalized Value': 'Gamut ALL Values'})
            
  #          gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
   #         gamut_df = gamut_df.drop(['Gamut_SKU', 'Grainger_SKU', 'Original Value', 'Normalized Value'], axis=1) #normalized values are collected as sample_value
            
       #     grainger_df['Gamut_Node_ID'] = int(node) #add correlating gamut node to grainger_df

            gamut_df = pd.merge(gamut_df, gamut_sample_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
         #   gamut_df['Category_ID'] = int(k)  #add grainger Category_ID column for gamut attributes
            gamut_df['Gamut_Attribute_Name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
            #create df based on names that match exactly
            gamut_df.to_csv ("F:/CGabriel/Grainger_Shorties/OUTPUT/gamut_test.csv")
            
            temp_df = pd.merge(grainger_df, gamut_df, on='Grainger_SKU')
            print(temp_df.info())
            temp_df['Grainger-Gamut Terminal Node Mapping'] = temp_df['Category_Name']+' -- '+ temp_df['Gamut_Node_Name']
       #     temp_df = temp_df.groupby(['Grainger_Attr_ID', 'Gamut_Attr_ID'])
#            temp_df = temp_df.drop_duplicates(subset='Grainger_Attr_ID', 'Gamut_Attr_ID')  #create list of unique grainger skus that feed into gamut query
     #       temp_df = temp_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value', 'Gamut_SKU',
      #                              'Grainger_SKU', 'Original Value', 'Normalized Value'])
        #    temp_df = pd.merge(grainger_df, gamut_df, left_on=['Grainger_Attribute_Name', 'Category_ID', 'Gamut_Node_ID'], 
         #                               right_on=['Gamut_Attribute_Name', 'Category_ID', 'Gamut_Node_ID'], how='outer')
            temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match

            df = pd.concat([df, temp_df], axis=0) #add prepped df for this gamut node to the final df


    return df  #where gamut_att_temp is the list of all normalized values for gamut attributes
    

#determine SKU or node search
search_level = 'cat.CATEGORY_ID'

gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()
grainger_skus = pd.DataFrame()

attribute_df = pd.DataFrame()
grainger_att_vals = pd.DataFrame()
grainger_sample_vals = pd.DataFrame()
gamut_att_vals = pd.DataFrame

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()
    
search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')

if data_type == 'grainger_query':
    if search_level == 'cat.CATEGORY_ID':
        for k in search_data:
            grainger_df = q.gcom.grainger_q(grainger_attr_query, search_level, k)
            if grainger_df.empty == False:
                grainger_att_vals, grainger_sample_vals = q.grainger_values(grainger_df)
                grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
                grainger_att_vals = grainger_att_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger ALL Values'})
                temp_df = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, k)
                attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                print ('Grainger ', k)
            else:
                print('No attribute data')
    else:
        for k in search_data:
            temp_df = q.grainger_nodes(k, search_level)
            grainger_skus = pd.concat([grainger_skus, temp_df], axis=0, sort=False)
            grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
            print('graigner L3s = ', grainger_l3)
        for k in grainger_l3:
            grainger_df = q.gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', k)
            if grainger_df.empty == False:
                grainger_att_vals, grainger_sample_vals = q.grainger_values(grainger_df)
                grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
                grainger_att_vals = grainger_att_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger ALL Values'})
                temp_df = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, k)
                attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                print ('Grainger ', k)
            else:
                print('No attribute data')   

#        attribute_df['Grainger-Gamut Terminal Node Mapping'] = attribute_df['Category_Name']+' -- '+attribute_df['Gamut_Node_Name']
attribute_df = attribute_df.drop(['Count_x', 'Count_y'], axis=1)

#attribute_df['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
#attribute_df['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
#attribute_df['Analyst Notes'] = ""
#attribute_df['Taxonomist Approved (yes/no)'] = ""
#attribute_df['Taxonomist Notes'] = ""

#pull.previous_match(attribute_df)

#data = process.attribute_name_match(attribute_df)

fd.attribute_match_data_out(settings.directory_name, attribute_df, search_level)

process.attribute_name_match(attribute_df)
                     
                     
print("--- {} seconds ---".format(round(time.time() - start_time, 2)))