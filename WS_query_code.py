# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:10:23 2019

@author: xcxg109
"""

import pandas as pd
import numpy as np
import re
import settings_NUMERIC as settings

"""CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
#from gamut_query import GamutQuery
from GWS_query import GWSQuery
#from GWS_TOOLBOX_query import GWSQuery

""" """

from grainger_query import GraingerQuery
from queries_NUMERIC import gws_basic_query, STEP_ETL_query, gamut_basic_query, gamut_attr_query


#gamut = GamutQuery()
gws = GWSQuery()
gcom = GraingerQuery()


gamut_def_query="""
    SELECT tax_att.name as "GWS_Attribute_Name"
         , tax_att.description as "Gamut_Attribute_Definition"
   
    FROM taxonomy_attribute tax_att

    WHERE {} IN ({})
        """


def gws_skus(grainger_skus):
    """get basic list of GWS SKUs to pull the related PIM nodes"""
    gws_sku_list = pd.DataFrame()
    gamut_sku_list = pd.DataFrame()
    
    sku_list = grainger_skus['Grainger_SKU'].tolist()
    
    if len(sku_list)>4000:
        num_lists = round(len(sku_list)/4000, 0)
        num_lists = int(num_lists)
    
        if num_lists == 1:
            num_lists = 2
        print('running GWS SKUs in {} batches'.format(num_lists))

        size = round(len(sku_list)/num_lists, 0)
        size = int(size)

        div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            print('batch {} of {}'.format(k+1, num_lists))
            gws_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
#            temp_df = gws.gws_q(gws_basic_query, 'tprod."gtPartNumber"', gws_skus)
            temp_gamut_df = gamut.gamut_q(gamut_basic_query, 'tprod."supplierSku"', gws_skus)
            
 #           gws_sku_list = pd.concat([gws_sku_list, temp_df], axis=0, sort=False) 
            gamut_sku_list = pd.concat([gamut_sku_list, temp_gamut_df], axis=0, sort=False) 
            
    else:
        gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
  #      gws_sku_list = gws.gws_q(gws_basic_query, 'tprod."gtPartNumber"', gws_skus)
        gamut_sku_list = gamut.gamut_q(gamut_basic_query, 'tprod."supplierSku"', gws_skus)

    return gamut_sku_list


def gws_atts(query, gws_node, query_type):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    
    df = gws.gws_q(query, query_type, gws_node)
    
    print('GWS ', gws_node)

    return df


def gamut_definition(gamut_node, query_type):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    print('gamut node = ', gamut_node)
    df = gamut.gamut_q(gamut_attr_query, query_type, gamut_node)
    
    print('Gamut ', gamut_node)

    return df


def grainger_nodes(grainger_node, search_level):
    """basic pull of all nodes if L2 or L3 is chosen"""
    df = pd.DataFrame()
    #pull basic details of all SKUs -- used for gathering L3s if user chooses L2 or L1
    df = gcom.grainger_q(STEP_ETL_query, search_level, grainger_node)
    
#    df.to_csv('C:/Users/xcxg109/NonDriveFiles/nodes.csv')
    
    return df

    
def grainger_values(df):
    """find the top 10 most used values for each attribute and return as sample_values"""
    all_vals = pd.DataFrame()
    comma_list = list()
    
    func_df = df.copy()
    func_df['Count'] =1
    func_df['Comma Separated Values'] = ''
    print('func_df = ', func_df.columns)
    atts = func_df['Grainger_Attribute_Name'].unique().tolist()
    
    # remove Item and Series from attribute counts (** specific terms)
    i = 'Item' in atts
    s = 'Series' in atts

    if i: 
        atts.remove('Item')
    if s: 
        atts.remove('Series')

    # remove 'Green' attributes based on general pattern match
    atts = [ x for x in atts if 'Green Certification' not in x ]
    atts = [ x for x in atts if 'Green Environmental' not in x ]

    vals = pd.DataFrame(func_df.groupby(['Grainger_Attr_ID', 'Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
 
    for attribute in atts:
        temp_df = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        temp_df = temp_df.sort_values(by=['Count'], ascending=[False])


        # build a list of comma separate attributes to help determine if a multi value is needed        

        subs = ','
        comma_list = temp_df['Grainger_Attribute_Value'].to_list()
        comma_list = [i for i in comma_list if subs in i] 
        
        regex = re.compile(r'\d+,\d+')
        exclude_list = list(filter(regex.match, comma_list))
                
        set_difference = set(comma_list) - set(exclude_list)
        diff = list(set_difference)
        diff = '; '.join(diff)

        temp_df['Comma Separated Values'] = diff
        
        # concat list items into string
        temp_df['Grainger ALL Values'] = '; '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        
        #pull the top 10 values and put into 'Sample_Values' field
        temp_att = temp_df.head(10)
        temp_df['Sample_Values'] = '; '.join(item for item in temp_att['Grainger_Attribute_Value'] if len(item)<250)
        all_vals = pd.concat([all_vals, temp_df], axis=0)

    if all_vals.empty == False:
        all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values', 'Comma Separated Values', 'Sample_Values']]
        all_vals = all_vals.drop_duplicates(subset=['Grainger_Attr_ID'])
#    df.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\hoist.csv')    

    return all_vals


def gamut_assign_nodes (grainger_df, gamut_df):
    """assign grainger node data to gamut columns"""
    
    att_list = []
    
    blue = grainger_df['STEP Blue Path'].unique()
    seg_ID = grainger_df['Segment_ID'].unique()
    seg_name = grainger_df['Segment_Name'].unique()
    fam_ID = grainger_df['Family_ID'].unique()
    fam_name = grainger_df['Family_Name'].unique()
    cat_ID = grainger_df['Category_ID'].unique()
    cat_name = grainger_df['Category_Name'].unique()
    
    atts = gamut_df['Gamut_Attribute_Name'].unique()
    att_list = [att for att in atts if att]
    att_list = np.char.strip(att_list)
    
    for att in att_list:
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_ID'] = cat_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'STEP Blue Path'] = blue
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_ID'] = seg_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_Name'] = seg_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_ID'] = fam_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_Name'] = fam_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_Name'] = cat_name

    return gamut_df


def grainger_assign_nodes (grainger_df, gamut_df):
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

#def grainger_assign_nodes (grainger_df, gws_df):
#    """assign gamut node data to grainger columns"""
    
#    att_list = []
    
#    node_ID = gws_df['GWS_Node_ID'].unique()
#    cat_ID = gws_df['GWS_Category_ID'].unique()
#    cat_name = gws_df['GWS_Category_Name'].unique()
#    node_name = gws_df['GWS_Node_Name'].unique()
#    pim_path = gws_df['GWS_PIM_Path'].unique()

 #   atts = grainger_df['Grainger_Attribute_Name'].unique()
 #   att_list = [att for att in atts if att]
 #   att_list = np.char.strip(att_list)

 #   for att in att_list:
 #       grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'GWS_Node_ID'] = node_ID
 #       grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'GWS_Category_ID'] = cat_ID
 #       grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'GWS_Category_Name'] = cat_name
 #       grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'GWS_Node_Name'] = node_name
 #       grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'GWS_PIM_Path'] = pim_path
    
 #   return grainger_df


def gws_assign_nodes (grainger_df, gws_df):
    """assign grainger node data to gamut columns"""
    
    att_list = []
    
    blue = grainger_df['STEP Blue Path'].unique()
    seg_ID = grainger_df['Segment_ID'].unique()
    seg_name = grainger_df['Segment_Name'].unique()
    fam_ID = grainger_df['Family_ID'].unique()
    fam_name = grainger_df['Family_Name'].unique()
    cat_ID = grainger_df['Category_ID'].unique()
    cat_name = grainger_df['Category_Name'].unique()
    
    atts = gws_df['GWS_Attribute_Name'].unique()
    att_list = [att for att in atts if att]
    att_list = np.char.strip(att_list)
    
    for att in att_list:
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Category_ID'] = cat_ID
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'STEP Blue Path'] = blue
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Segment_ID'] = seg_ID
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Segment_Name'] = seg_name
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Family_ID'] = fam_ID
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Family_Name'] = fam_name
        gws_df.loc[gws_df['GWS_Attribute_Name'] == att, 'Category_Name'] = cat_name

    return gws_df


def get_LOVs(filename):
    """read in LOV file from my F: drive. If we want to use the URL commented out below, need to upload the current
    version of the file to github. If an updated report is available, need to save a copy with report headers and 
    footers removed, but no other changes necessary"""    
        
    df = pd.read_csv(filename)
    df = df[['AttributeID', 'GIS_US_ENG']]  # other contexts = 'AGI_CA_INTL_ENG', 'AGI_CA_FR', 'GISMX_ES_MX'

    df['AttributeID'] = df['AttributeID'].str.replace('_ATTR', '')
    df['AttributeID'] = df['AttributeID'].str.replace('_GATTR', '')
    
    df['AttributeID'] = df['AttributeID'].astype(int)
    
    df.reset_index(drop=True, inplace=True)

    df['GIS_US_ENG'].fillna('', inplace=True)
    
    df = df.rename(columns={'GIS_US_ENG':'Value'})
    df.dropna(subset=['Value'], inplace=True)

    # explode values column, creating multiple rows for multivaules based on comma separation
    lst_col = 'Value' 
    x = df.assign(**{lst_col:df[lst_col].str.split(',')})   

    lovs = pd.DataFrame({col:np.repeat(x[col].values, x[lst_col].str.len()) for col in x.columns.difference([lst_col]) \
                   }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]

    # remove white spaces before and after values after comma separation
    lovs['Value'] = lovs['Value'].str.strip()

    # create a set of unique values for each AttributeID, sort the values, then join with ; -
    lovs = lovs.groupby('AttributeID')['Value'].apply(set).reset_index()
    lovs['Value'] = lovs['Value'].apply(sorted)
    lovs['Value'] = lovs['Value'].transform(lambda x: '; '.join(x))
    
    lovs = lovs[['AttributeID', 'Value']]
    
    lov_list = lovs['AttributeID'].tolist()
    lov_list = set(lov_list)

    return lovs, lov_list


def get_att_values():
    """ read in externally generated file of all attribute values at the L1 level. file format exported from
    teradata SQL assistant as tab delimited text """
   
    filename = settings.choose_file()

    delim = '|'
    
    """ ignore errors on import lines (message will print when loading) """
    
    df['Count'] = 1
        
#    df.to_csv('C:/Users/xcxg109/NonDriveFiles/allCats.csv')
    
    return df