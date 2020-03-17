# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:10:23 2019

@author: xcxg109
"""

import pandas as pd
"""CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
#from gamut_query_15 import GamutQuery_15
from GWS_query import GWSQuery
""" """
from grainger_query import GraingerQuery
from queries_PIM import gamut_basic_query, grainger_attr_query, grainger_name_query, grainger_basic_query


gcom = GraingerQuery()

"""CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
#gamut = GamutQuery_15() #OLD pulling from 1.5 system
gamut = GWSQuery()
""" """

def gamut_skus(grainger_skus):
    """get basic list of gamut SKUs to pull the related PIM nodes"""
    sku_list = grainger_skus['Grainger_SKU'].tolist()
    gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)

    """CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
#    gamut_sku_list = gamut.gamut_q15(gamut_basic_query, 'tprod."supplierSku"', gamut_skus) #OLD pulling from 1.5 system
    gamut_sku_list = gamut.gws_q(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)
    """ """
        
    return gamut_sku_list


def gamut_atts(query, node, query_type):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    
    """CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
#    df = gamut.gamut_q15(query, query_type, node)    #OLD pulling from 1.5 system
    df = gamut.gws_q(query, query_type, node)
    """ """
    
    print('Gamut ', node)

    return df


def grainger_nodes(node, search_level):
    """basic pull of all nodes if L2 or L3 is chosen"""
    df = pd.DataFrame()
    #pull basic details of all SKUs -- used for gathering L3s if user chooses L2 or L1
    df = gcom.grainger_q(grainger_basic_query, search_level, node)
    
    return df

    
def grainger_atts(node):
    """pull grainger attributes based on Categiry ID"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    df = gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', node)

    return df


def grainger_by_name(att):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()

    if isinstance(att, int):#k.isdigit() == True:
        pass
    else:
        att = "'" + str(att) + "'"
    df = gcom.grainger_q(grainger_name_query, 'attr.DESCRIPTOR_NAME', att)

    if df.empty == True:
        print('GRAINGER_BY_NAME with {} = No results returned', att)
        
    return df


def grainger_values(df):
    """find the top 5 most used values for each attribute and return as sample_values"""
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    all_vals = pd.DataFrame()
    
    df['Count'] =1
    atts = df['Grainger_Attribute_Name'].unique()
    
    vals = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()

    for attribute in atts:
        temp_att = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        #pull the top 10 values and put into 'sample' field
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(10)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df['Grainger ALL Values'] = ' '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        all_vals= pd.concat([all_vals, temp_df], axis=0)

    top_vals = top_vals.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Value'].apply('; '.join).reset_index()
    
    #vals = vals.drop(['Count'], axis=1)
    #vals = vals.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Value'].apply('; '.join).reset_index()
    all_vals = all_vals.drop_duplicates(subset='Grainger_Attr_ID')
    all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values']]
        
    return all_vals, top_vals


def gamut_values(query, node, query_type):
    """find the top 5 most used values for each attribute and return as sample_values"""
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    all_vals = pd.DataFrame()
    
    """CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
#    df = gamut.gamut_q15(query, query_type, node)   #OLD pulling from 1.5 system
    df = gamut.gws_q(query, query_type, node)
    """ """

    if df.empty==False:
        df['Count'] = 1
        atts = df['Gamut_Attribute_Name'].unique()
    
        vals = pd.DataFrame(df.groupby(['Gamut_Attribute_Name', 'Normalized Value'])['Count'].sum())
        vals = vals.reset_index()
 
        for attribute in atts:
            temp_att = vals.loc[vals['Gamut_Attribute_Name']== attribute]
            #pull the top 10 values and put into 'sample' field
            temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(10)
            top_vals = pd.concat([top_vals, temp_att], axis=0)
            #put all attribute values into a single string for TF-IDF processing later            
            temp_df = df.loc[df['Gamut_Attribute_Name']== attribute]
            temp_df['Gamut ALL Values'] = ' '.join(item for item in temp_df['Normalized Value'] if item)
            all_vals= pd.concat([all_vals, temp_df], axis=0)
                        
        top_vals = top_vals.groupby('Gamut_Attribute_Name')['Normalized Value'].apply('; '.join).reset_index()
        
        all_vals = all_vals.drop_duplicates(subset='Gamut_Attr_ID')
        all_vals = all_vals[['Gamut_Attr_ID', 'Gamut ALL Values']]
    else:
        print('Gamut node {} NO VALUES'.format(node))
        
    return all_vals, top_vals