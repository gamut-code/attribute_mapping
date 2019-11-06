# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 16:39:52 2019

@author: xcxg109
"""

import pandas as pd
import query_code as q
import data_process as process
import file_data_att as fd
from typing import Dict
import settings
from queries_PIM import grainger_attr_query, gamut_attr_values, gamut_attr_query
import time


pd.options.mode.chained_assignment = None


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


def gamut_process(node, gamut_dict: Dict, k):
    """if gamut node has not been previously processed (in gamut_dict), process and add it to the dictionary"""
    gamut_sample_vals = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()

    gamut_df = q.gamut_atts(gamut_attr_query, node, 'tax.id')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\
    
    if gamut_df.empty==False:
        gamut_att_vals, gamut_sample_vals = q.gamut_values(gamut_attr_values, node, 'tax.id') #gamut_values exports a list of --all-- normalized values and sample_values
        
        if gamut_att_vals.empty==False:
            gamut_sample_vals = gamut_sample_vals.rename(columns={'Normalized Value': 'Gamut Attribute Sample Values'})
            gamut_df = pd.merge(gamut_df, gamut_sample_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attr_ID'])  #add t0p 5 normalized values to report

        gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
        
        gamut_dict[node] = gamut_df #store the processed df in dict for future reference
 
    else:
        print('{} EMPTY DATAFRAME'.format(node))    
        
    return gamut_dict, gamut_df


def grainger_assign_nodes (grainger_df, gamut_df):
    """assign gamut node data to grainger columns"""
    
    att_list = []
    
    node_ID = gamut_df['Gamut_Node_ID']
    cat_ID = gamut_df['Gamut_Category_ID']
    cat_name = gamut_df['Gamut_Category_Name']
    node_name = gamut_df['Gamut_Node_Name']
    pim_path = gamut_df['Gamut_PIM_Path']

    att_list = grainger_df['Grainger_Attribute_Name'].unique()
    
    for att in att_list:
        grainger_df.loc[grainger_df['Grainger_Attribute_Name'] == att, 'Gamut_Node_ID'] = node_ID
        grainger_df.loc[grainger_df['Grainger_Attribute_Name'] == att, 'Gamut_Category_ID'] = cat_ID
        grainger_df.loc[grainger_df['Grainger_Attribute_Name'] == att, 'Gamut_Category_Name'] = cat_name
        grainger_df.loc[grainger_df['Grainger_Attribute_Name'] == att, 'Gamut_Node_Name'] = node_name
        grainger_df.loc[grainger_df['Grainger_Attribute_Name'] == att, 'Gamut_PIM_Path'] = pim_path
    
    return grainger_df


def gamut_assign_nodes (grainger_df, gamut_df):
    """assign grainger node data to gamut columns"""
    
    att_list = []
    
    blue = grainger_df['Grainger Blue Path'].unique()
    seg_ID = grainger_df['Segment_ID'].unique()
    seg_name = grainger_df['Segment_Name'].unique()
    fam_ID = grainger_df['Family_ID'].unique()
    fam_name = grainger_df['Family_Name'].unique()
    cat_ID = grainger_df['Category_ID'].unique()
    cat_name = grainger_df['Category_Name'].unique()
    
    att_list = gamut_df['Gamut_Attribute_Name'].unique()
    
    for att in att_list:
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_ID'] = cat_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Grainger Blue Path'] = blue
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_ID'] = seg_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_Name'] = seg_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_ID'] = fam_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_Name'] = fam_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_Name'] = cat_name
    
    return gamut_df


def grainger_process(grainger_df, grainger_sample, grainger_all, gamut_dict: Dict, k):
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
    
    grainger_df['alt_grainger_name'] = process.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge
    #grainger_df.to_csv ("F:/CGabriel/Grainger_Shorties/OUTPUT/grainger_test.csv")
    
    gamut_skus = q.gamut_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull
    if gamut_skus.empty==False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node 
        gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        print('GAMUT L3s ', gamut_l3)
        
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
                grainger_df = grainger_assign_nodes(grainger_df, gamut_df)
                gamut_df = gamut_assign_nodes(grainger_df, gamut_df)
 
                skus = gamut_skus[gamut_skus['Gamut_Node_ID'] == node]
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')
                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 
                temp_df['grainger_sku_count'] = grainger_sku_count
                temp_df['gamut_sku_count'] = len(skus)#temp_skus['Gamut_SKU']
                temp_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '+node_name
                temp_df['Gamut/Grainger SKU Counts'] = temp_df['gamut_sku_count'].map(str)+' / '+temp_df['grainger_sku_count'].map(str)
                
                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')
            else:
                print('Gamut Node {} EMPTY DATAFRAME'.format(node))

    else:
        print('No Gamut SKUs for Grainger node {}'.format(k))
        
    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes
    

#determine SKU or node search
search_level = 'cat.CATEGORY_ID'

gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()
grainger_skus = pd.DataFrame()

attribute_df = pd.DataFrame()
grainger_att_vals = pd.DataFrame()
grainger_sample_vals = pd.DataFrame()
gamut_att_vals = pd.DataFrame()
gamut_dict = dict()

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
                temp_df, gamut_dict = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, gamut_dict, k)
                attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                print ('Grainger ', k)
            else:
                print('No attribute data')
    else:
        for k in search_data:
            print('K = ', k)
            grainger_skus = q.grainger_nodes(k, search_level)
            #grainger_skus = pd.concat([grainger_skus, temp_df], axis=0, sort=False)
            grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
            print('grainger L3s = ', grainger_l3)
            for j in grainger_l3:
                grainger_df = q.gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', j)
                if grainger_df.empty == False:
                    grainger_att_vals, grainger_sample_vals = q.grainger_values(grainger_df)
                    grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
                    temp_df, gamut_dict = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, gamut_dict, j)
                    attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                    print ('Grainger ', j)
                else:
                    print('Grainger node {} All SKUs are R4, R9, or discontinued'.format(j)) 
            print("--- {} seconds ---".format(round(time.time() - start_time, 2)))


attribute_df = attribute_df.drop(['Count'], axis=1)

fd.attribute_match_data_out(settings.directory_name, attribute_df, search_level)

                     

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))