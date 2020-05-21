# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:10:23 2019

@author: xcxg109
"""

import pandas as pd
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_NUMERIC import gamut_basic_query, grainger_attr_query, grainger_basic_query


gcom = GraingerQuery()

gamut = GWSQuery()


def gamut_skus(grainger_skus):
    """get basic list of gamut SKUs to pull the related PIM nodes"""
    gamut_sku_list = pd.DataFrame()
    
    sku_list = grainger_skus['Grainger_SKU'].tolist()
    
    if len(sku_list)>20000:
        num_lists = round(len(sku_list)/20000, 0)
        num_lists = int(num_lists)
    
        if num_lists == 1:
            num_lists = 2
        print('running SKUs in {} batches'.format(num_lists))

        size = round(len(sku_list)/num_lists, 0)
        size = int(size)

        div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            gamut_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
            temp_df = gamut.gws_q(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)
            gamut_sku_list = pd.concat([gamut_sku_list, temp_df], axis=0, sort=False) 
    else:
        gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
        gamut_sku_list = gamut.gws_q(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)

    return gamut_sku_list


def gamut_atts(query, node, query_type):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    
    df = gamut.gws_q(query, query_type, node)
    
    print('GWS ', node)

    return df


def grainger_nodes(node, search_level):
    """basic pull of all nodes if L2 or L3 is chosen"""
    df = pd.DataFrame()
    #pull basic details of all SKUs -- used for gathering L3s if user chooses L2 or L1
    df = gcom.grainger_q(grainger_basic_query, search_level, node)
    
    return df

    
def grainger_values(df):
    """find the top 10 most used values for each attribute and return as sample_values"""
    all_vals = pd.DataFrame()
    
    df['Count'] =1
    atts = df['Grainger_Attribute_Name'].unique()

    vals = pd.DataFrame(df.groupby(['Grainger_Attr_ID', 'Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
 
    for attribute in atts:
        temp_df = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        temp_df = temp_df.sort_values(by=['Count'], ascending=[False])
        temp_df['Grainger ALL Values'] = '; '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        
        #pull the top 10 values and put into 'Sample Values' field
        temp_att = temp_df.head(10)
        temp_df['Sample Values'] = '; '.join(item for item in temp_att['Grainger_Attribute_Value'] if item)
        all_vals = pd.concat([all_vals, temp_df], axis=0)

    all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values', 'Sample Values']]

    return all_vals


def gamut_values(query, node, query_type):
    """find the top 10 most used values for each attribute and return as sample_values"""
    all_vals = pd.DataFrame()
    
    df = gamut.gws_q(query, query_type, node)

    if df.empty==False:
        df['Count'] = 1
        atts = df['Gamut_Attribute_Name'].unique()
    
        vals = pd.DataFrame(df.groupby(['Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Normalized Value'])['Count'].sum())
        vals = vals.reset_index()
 
        for attribute in atts:
            temp_df = vals.loc[vals['Gamut_Attribute_Name']== attribute]
            temp_df = temp_df.sort_values(by=['Count'], ascending=[False])
#            temp_df['Gamut ALL Values'] = '; '.join(item for item in temp_df['Normalized Value'] if item)

            #pull the top 10 values and put into 'Gamut Sample Values' field
            temp_att = temp_df.head(10)
            temp_df['Gamut Sample Values'] = '; '.join(item for item in temp_att['Normalized Value'] if item)
            all_vals = pd.concat([all_vals, temp_df], axis=0)

        all_vals = all_vals[['Gamut_Attr_ID', 'Gamut Sample Values']]
    else:
        print('GWS node {} NO VALUES'.format(node))
        
    return all_vals