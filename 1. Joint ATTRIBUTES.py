# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 16:39:52 2019

@author: xcxg109
"""

import pandas as pd
from gamut_query_15 import GamutQuery_15
from grainger_query import GraingerQuery
from queries_PIM import gamut_basic_query, grainger_attr_query, gamut_attr_query
import file_data as fd
import settings
from pathlib import Path
import requests
import io


gcom = GraingerQuery()
gamut = GamutQuery_15()


def get_attributeMatch():
    """get previous attribute matching data (from Github https://github.com/gamut-code/descriptions-queries/tree/master/Python) for proceesing"""
    url = 'https://raw.githubusercontent.com/gamut-code/descriptions-queries/master/Python/MATCHING%20ATTRIBUTES%20grainger_gamut15.csv?token=AFQPQ37ULXGT6FO2PB2LW525P7XRW'

    data_file = requests.get(url).content

    match_df = pd.read_csv(io.StringIO(data_file.decode('ISO-8859-1')))
    pd.options.mode.chained_assignment = None
    
    return match_df
    

def gamut_skus(grainger_skus):
    """get basic list of gamut SKUs to pull the related PIM nodes"""
    sku_list = grainger_skus['Grainger_SKU'].tolist()
    gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    gamut_sku_list = gamut.gamut_q15(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)
    
    return gamut_sku_list


def gamut_atts(node):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    df = gamut.gamut_q15(gamut_attr_query, 'tprod."categoryId"', node)
        #df = pd.concat([df, temp_df], axis=0)
    print('Gamut ', node)

    return df


def grainger_values(grainger_df):
    """find the top 5 most used values for each attribute and return as sample_values"""
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    
    grainger_df['Count'] =1
    atts = grainger_df['Grainger_Attribute_Name'].unique()
    
    vals = pd.DataFrame(grainger_df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
    
    #top_vals = vals.nlargest(5,['Grainger_Attribute_Name','Count'])
    #top_vals = df.groupby('Grainger_Attribute_Name', group_keys=False).apply(lambda x: x.nlargest(5, 'Count'))
    for attribute in atts:
        temp_att = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(5)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        
    top_vals = top_vals.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Value'].apply('; '.join).reset_index()
        
    return top_vals


def gamut_values(gamut_df):
    """find the top 5 most used values for each attribute and return as sample_values"""
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    
    gamut_df['Count'] =1
    atts = gamut_df['Gamut_Attribute_Name'].unique()
    
    vals = pd.DataFrame(gamut_df.groupby(['Gamut_Attribute_Name', 'Normalized Value'])['Count'].sum())
    vals = vals.reset_index()
    
    #top_vals = vals.nlargest(5,['Grainger_Attribute_Name','Count'])
    #top_vals = df.groupby('Grainger_Attribute_Name', group_keys=False).apply(lambda x: x.nlargest(5, 'Count'))
    for attribute in atts:
        temp_att = vals.loc[vals['Gamut_Attribute_Name']== attribute]
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(5)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        
    top_vals = top_vals.groupby('Gamut_Attribute_Name')['Normalized Value'].apply('; '.join).reset_index()
        
    return top_vals


def grainger_process(grainger_df, grainger_sample, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    df = pd.DataFrame()
    gamut_sample = pd.DataFrame()
    
    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gamut query
    grainger_df = grainger_df.drop_duplicates(subset=['L3', 'Grainger_Attr_ID'])  #group by L3 and attribute name and keep unique
    grainger_df['Grainger Blue Path'] = grainger_df['Segment_Name']+' > '+grainger_df['Family_Name']+' > '+grainger_df['Category_Name'] #compile grainger path details
    grainger_df = grainger_df.drop(columns=['Grainger_SKU', 'Grainger_Attribute_Value']) #remove unneeded columns
    grainger_df = grainger_df.merge(grainger_sample, on=['Grainger_Attribute_Name'])

    grainger_df['Grainger_Attribute_Name'] = grainger_df['Grainger_Attribute_Name'].str.lower()  #prep att name for merge
    
    gamut_sku_list = gamut_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull

    if gamut_sku_list.empty == False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node
        gamut_l3 = gamut_sku_list['Gamut Node ID'].unique()  #create list of pim nodes to pull
        for node in gamut_l3:
            gamut_df = gamut_atts(node)  #get gamut attribute values for each gamut_l3 node
            gamut_sample = gamut_values(gamut_df)
            gamut_sample = gamut_sample.rename(columns={'Normalized Value': 'Gamut Attribute Sample Values'})
            gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
            gamut_df = gamut_df.drop(columns=['Gamut_SKU', 'Grainger_SKU', 'Original Value', 'Normalized Value'])
            grainger_df['Gamut Node ID'] = int(node)
            gamut_df = gamut_df.merge(gamut_sample, on=['Gamut_Attribute_Name'])
            gamut_df['L3'] = int(k)  #add grainger L3 column for gamut attributes
            gamut_df['Gamut_Attribute_Name'] = gamut_df['Gamut_Attribute_Name'].str.lower()  #prep att name for merge
            #create df based on names that match exactly
            temp_df = grainger_df.merge(gamut_df, left_on=['Grainger_Attribute_Name', 'L3', 'Gamut Node ID'], right_on=['Gamut_Attribute_Name', 'L3', 'Gamut Node ID'], how='outer')
            df = pd.concat([df, temp_df], axis=0)

    return df
    
def isBlank (myString):
    return not (myString and pd.isnull(myString))


def determine_match(df):
    """compare data colected from matching file (match_df) with grainger and gamut data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""
    
    for index, row in attribute_df.iterrows():
        if isBlank(row['Grainger_Attribute_Name']) == False:
            if isBlank(row['Gamut_Attribute_Name']) == False:
               df.loc[index,'Matching'] = 'Match'
            elif isBlank(row['Gamut_Attribute_Name']) == True:
                df.loc[index,'Matching'] = 'Grainger only'
        elif isBlank(row['Grainger_Attribute_Name']) == True:
            if isBlank(row['Gamut_Attribute_Name']) == False:
                df.loc[index,'Matching'] = 'Gamut only'
                
    return df




#determine SKU or node search
search_level = 'cat.CATEGORY_ID'
data_type = fd.search_type()

gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()
attribute_df = pd.DataFrame()
grainger_sample = pd.DataFrame()

match_df = get_attributeMatch()

gamut_l3 = dict()


if data_type == 'node':
    search_level = fd.blue_search_level()
    
search_data = fd.data_in(data_type, settings.directory_name)

if data_type == 'node':
    for k in search_data:
        grainger_df = gcom.grainger_q(grainger_attr_query, search_level, k)
        if grainger_df.empty == False:
            grainger_sample = grainger_values(grainger_df)
          # grainger_sample = grainger_df.drop(columns=['Count'])
            grainger_sample = grainger_sample.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
            temp_df = grainger_process(grainger_df, grainger_sample, k)
            attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
        else:
            print('No attribute data')
        attribute_df['Grainger-Gamut Terminal Node Mapping'] = attribute_df['Category_Name']+' -- '+attribute_df['Gamut Node Name']
        attribute_df = attribute_df.drop(['Count_x', 'Count_y'], axis=1)
        print ('Grainger ', k)

attribute_df['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
attribute_df['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
attribute_df['Analyst Notes'] = ""
attribute_df['Taxonomist Notes'] = ""

attribute_df = determine_match(attribute_df)


fd.attribute_match_data_out(settings.directory_name, attribute_df, search_level)