# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 10:10:23 2019

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_NUMERIC import gamut_basic_query, grainger_basic_query


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
    comma_list = list()
    
    func_df = df.copy()
    func_df['Count'] =1
    func_df['Comma Separated Values'] = ''
    
    atts = func_df['Grainger_Attribute_Name'].unique()

    vals = pd.DataFrame(func_df.groupby(['Grainger_Attr_ID', 'Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
 
    for attribute in atts:
        temp_df = vals.loc[vals['Grainger_Attribute_Name']== attribute]
        temp_df = temp_df.sort_values(by=['Count'], ascending=[False])


        # build a list of comma separate attributes to help determine if a multi value is needed        
        subs = ','
        comma_list = temp_df['Grainger_Attribute_Value'].to_list()
        comma_list = [item for item in comma_list if subs in item]                         
        temp_df['Comma Separated Values'] = '; '.join(comma_list)

        # concat list items into string
        temp_df['Grainger ALL Values'] = '; '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        
        #pull the top 10 values and put into 'Sample Values' field
        temp_att = temp_df.head(10)
        temp_df['Sample Values'] = '; '.join(item for item in temp_att['Grainger_Attribute_Value'] if item)
        all_vals = pd.concat([all_vals, temp_df], axis=0)

    all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values', 'Comma Separated Values', 'Sample Values']]
    all_vals = all_vals.drop_duplicates(subset=['Grainger_Attr_ID'])
    df.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\hoist.csv')    
    func_df.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\FOIST.csv')    

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

            #pull the top 10 values and put into 'Gamut Sample Values' field
            temp_att = temp_df.head(10)
            temp_df['Gamut Top Attribute Values'] = '; '.join(item for item in temp_att['Normalized Value'] if item)
            all_vals = pd.concat([all_vals, temp_df], axis=0)

        all_vals = all_vals[['Gamut_Attr_ID', 'Gamut Top Attribute Values']]
        all_vals = all_vals.drop_duplicates(subset=['Gamut_Attr_ID'])

    else:
        print('GWS node {} NO VALUES'.format(node))
        
    return all_vals


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


def get_LOVs():
    """read in LOV file from my F: drive. If we want to use the URL commented out below, need to upload the current
    version of the file to github. If an updated report is available, need to save a copy with report headers and 
    footers removed, but no other changes necessary"""    
#    lov_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/LOV_list.csv'
#    data_file = requests.get(lov_groups_url).content
#    lov_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

    filename = 'F:\CGabriel\LOV_list.csv'
    
    df = pd.read_csv(filename)
    df = df[['AttributeID', 'GIS_US_ENG', 'AGI_CA_INTL_ENG', 'AGI_CA_FR', 'GISMX_ES_MX']]

    df['AttributeID'] = df['AttributeID'].str.replace('_ATTR', '')
    df.reset_index(drop=True, inplace=True)

    df['GIS_US_ENG'].fillna('', inplace=True)
    df['AGI_CA_INTL_ENG'].fillna('', inplace=True)
    df['AGI_CA_FR'].fillna('', inplace=True)
    df['GISMX_ES_MX'].fillna('', inplace=True)
    
    df['Value'] = ''
    
    for row in df.itertuples():
        us_eng = str(row.GIS_US_ENG)
        agi_eng = str(row.AGI_CA_INTL_ENG)
        fr = str(row.AGI_CA_FR)
        span = str(row.GISMX_ES_MX)
        
        # create a single source of truth for values, prioritizing STEP's GIS_US_ENG context
        if us_eng != '':
            df.at[row.Index,'Value'] = us_eng
        elif agi_eng != '':
            df.at[row.Index,'Value'] = agi_eng
        elif fr != '':
            df.at[row.Index,'Value'] = fr
        elif span != '':
            df.at[row.Index,'Value'] = span
        else:
            id = str(row.AttributeID)

    # if no values are present for any available context, drop the LOV
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
    
    return lovs
