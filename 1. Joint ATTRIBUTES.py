# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 16:39:52 2019

@author: xcxg109
"""

import pandas as pd
from gamut_query_15 import GamutQuery_15
from grainger_query import GraingerQuery
from queries_PIM import gamut_basic_query, grainger_attr_query, gamut_attr_query
import attribute_data_pull as pull
import file_data_att as fd
import settings

pd.options.mode.chained_assignment = None

gcom = GraingerQuery()
gamut = GamutQuery_15()
    

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

    for attribute in atts:
        temp_att = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(5)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        
    top_vals = top_vals.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Value'].apply('; '.join).reset_index()
    
    vals = vals.drop(['Count'], axis=1)
    vals = vals.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Value'].apply('; '.join).reset_index()
    
    return vals, top_vals


def gamut_values(gamut_df):
    """find the top 5 most used values for each attribute and return as sample_values"""
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    
    gamut_df['Count'] = 1
    atts = gamut_df['Gamut_Attribute_Name'].unique()
    
    vals = pd.DataFrame(gamut_df.groupby(['Gamut_Attribute_Name', 'Normalized Value'])['Count'].sum())
    vals = vals.reset_index()
    
    for attribute in atts:
        temp_att = vals.loc[vals['Gamut_Attribute_Name']== attribute]
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(5)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        
    top_vals = top_vals.groupby('Gamut_Attribute_Name')['Normalized Value'].apply('; '.join).reset_index()
        
    vals = vals.drop(['Count'], axis=1)
    vals = vals.groupby('Gamut_Attribute_Name')['Normalized Value'].apply('; '.join).reset_index()
    
    return vals, top_vals


def match_category(df):
    """compare data colected from matching file (match_df) with grainger and gamut data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""

    for row in df.itertuples():
        if (row.Index, row.Grainger_Attribute_Name) == (row.Index, row.Gamut_Attribute_Name):
            df.at[row.Index,'Matching'] = 'Match'
        elif isBlank(row.Grainger_Attribute_Name) == False:
            if isBlank(row.Gamut_Attribute_Name) == True:
                df.at[row.Index,'Matching'] = 'Grainger only'
        elif isBlank(row.Grainger_Attribute_Name) == True:
            if isBlank(row.Gamut_Attribute_Name) == False:
                df.at[row.Index,'Matching'] = 'Gamut only'
             
    return df


def grainger_process(grainger_df, grainger_sample, grainger_all, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    df = pd.DataFrame()
    gamut_sample_vals = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()
    
    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gamut query
    
    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df['Grainger Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']
    grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns
    grainger_df = pd.merge(grainger_df, grainger_sample, on=['Grainger_Attribute_Name'])
    grainger_df = pd.merge(grainger_df, grainger_all, on=['Grainger_Attribute_Name'])
    
    grainger_df['Grainger_Attribute_Name'] = pull.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge
    
    gamut_sku_list = gamut_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull

    if gamut_sku_list.empty == False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node
        gamut_l3 = gamut_sku_list['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        for node in gamut_l3:
            gamut_df = gamut_atts(node)  #get gamut attribute values for each gamut_l3 node
            gamut_att_vals, gamut_sample_vals = gamut_values(gamut_df) #gamut_values exports a list of --all-- normalized values (temp_df) and sample_values
#            temp_df, gamut_sample_vals = gamut_values(gamut_df, grainger_df) #gamut_values exports a list of --all-- normalized values (temp_df) and sample_values
#            gamut_att_temp = pd.concat([gamut_att_temp, temp_df], axis=0, sort=False) #create list of gamut attribute values for all nodes
            gamut_sample_vals = gamut_sample_vals.rename(columns={'Normalized Value': 'Gamut Attribute Sample Values'})
            gamut_att_vals = gamut_att_vals.rename(columns={'Normalized Value': 'Gamut ALL Values'})
            gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
            gamut_df = gamut_df.drop(['Gamut_SKU', 'Grainger_SKU', 'Original Value', 'Normalized Value'], axis=1) #normalized values are collected as sample_value
            grainger_df['Gamut_Node_ID'] = int(node) #add correlating gamut node to grainger_df
            gamut_df = pd.merge(gamut_df, gamut_sample_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attribute_Name'])  #add t0p 5 normalized values to report
            gamut_df['Category_ID'] = int(k)  #add grainger Category_ID column for gamut attributes
            gamut_df['Gamut_Attribute_Name'] = pull.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
            #create df based on names that match exactly
            temp_df = pd.merge(grainger_df, gamut_df, left_on=['Grainger_Attribute_Name', 'Category_ID', 'Gamut_Node_ID'], 
                                        right_on=['Gamut_Attribute_Name', 'Category_ID', 'Gamut_Node_ID'], how='outer')
            temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match
            df = pd.concat([df, temp_df], axis=0) #add prepped df for this gamut node to the final df

    return df  #where gamut_att_temp is the list of all normalized values for gamut attributes
    

def isBlank (myString):
    return (myString and pd.isnull(myString))



#determine SKU or node search
search_level = 'cat.CATEGORY_ID'
data_type = fd.search_type()

gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()

attribute_df = pd.DataFrame()
grainger_att_vals = pd.DataFrame()
grainger_sample_vals = pd.DataFrame()
gamut_att_vals = pd.DataFrame

gamut_l3 = dict()


if data_type == 'node':
    search_level = fd.blue_search_level()
    
search_data = fd.data_in(data_type, settings.directory_name)

if data_type == 'node':
    for k in search_data:
        grainger_df = gcom.grainger_q(grainger_attr_query, search_level, k)
        if grainger_df.empty == False:
            grainger_att_vals, grainger_sample_vals = grainger_values(grainger_df)
            grainger_sample_vals = grainger_sample_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger Attribute Sample Values'})
            grainger_att_vals = grainger_att_vals.rename(columns={'Grainger_Attribute_Value': 'Grainger ALL Values'})
            temp_df = grainger_process(grainger_df, grainger_sample_vals, grainger_att_vals, k)
            attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
        else:
            print('No attribute data')
        attribute_df['Grainger-Gamut Terminal Node Mapping'] = attribute_df['Category_Name']+' -- '+attribute_df['Gamut_Node_Name']
        attribute_df = attribute_df.drop(['Count_x', 'Count_y'], axis=1)
        print ('Grainger ', k)

attribute_df['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
attribute_df['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
attribute_df['Analyst Notes'] = ""
attribute_df['Taxonomist Review (yes/no approval)'] = ""
attribute_df['Taxonomist Notes'] = ""

#test = pull.determine_match(attribute_df, grainger_att_vals, gamut_att_vals)

fd.attribute_match_data_out(settings.directory_name, attribute_df, search_level)

#test.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\test.csv')