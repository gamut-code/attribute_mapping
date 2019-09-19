# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:10:23 2019

@author: xcxg109
"""

import pandas as pd
from gamut_query_15 import GamutQuery_15
from grainger_query import GraingerQuery
from queries_PIM import gamut_basic_query, gamut_attr_query


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