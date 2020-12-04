# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 12:53:25 2020

Inputs: upload of Gamut and WS attribute data files (queries to be run externally, but included here for reference)

Function: comparison of Nodes and Attributes between WS and Gamut, matching based on attribute naming conventions

@author: xcxg109
"""

import pandas as pd
import re
import time
import math
from typing import Dict
import data_process as process
import file_data_GWS as fd
import WS_query_code as q
from gamut_ORIGINAL_query import GamutQuery
from queries_original_Gamut import gamut_usage_query
import WS_query_code as q


# query to get WS attributes & values
gws_attr_values="""
        WITH RECURSIVE tax AS (
                SELECT  id,
            name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
                FROM    taxonomy_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false

                UNION ALL

                SELECT  category.id,
            category.name,
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att.id as "WS_Attr_ID"
        , tprodvalue.id as "WS_Attr_Value_ID"
        , tax_att."multiValue" as "Multivalue"
        , tax_att."dataType" as "Data_Type"
  	    , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tax_att.description as "WS_Attribute_Definition"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "WS_Original_Value"
        , tprodvalue.unit as "WS_Original_Unit"
        , tprodvalue."valueNormalized" as "WS_Normalized_Value"
        , tprodvalue."unitNormalized" as "WS_Normalized_Unit"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att.deleted = 'false'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        AND tprodvalue.deleted = 'false'

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """

# gamut version of the attribute/values query
gamut_attr_values="""
        WITH RECURSIVE tax AS (
                SELECT  id,
            name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
                FROM    taxonomy_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false

                UNION ALL

                SELECT  category.id,
            category.name,
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "Gamut_PIM_Path"
        , tax.ancestors[1] as "Gamut_Category_ID"  
        , tax.ancestor_names[1] as "Gamut_Category_Name"
        , tprod."categoryId" AS "Gamut_Node_ID"
        , tax.name as "Gamut_Node_Name"
        , tprod."gtPartNumber" as "Gamut_SKU"
        , tprod."supplierSku" as "Grainger_SKU"
        , tax_att.id as "Gamut_Attr_ID"
        , tax_att."multiValue" as "Multivalue"
        , tax_att."dataType" as "Data_Type"
  	    , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tax_att.description as "Gamut_Attribute_Definition"
        , tax_att.name as "Gamut_Attribute_Name"
        , tprodvalue.value as "Gamut_Original Value"
        , tprodvalue.unit as "Gamut_Original_Unit"
        , tprodvalue."valueNormalized" as "Gamut_Normalized Value"
        , tprodvalue."unitNormalized" as "Gamut_Normalized_Unit"

    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE {} IN ({})
        """


gamut = GamutQuery()


def get_gamut_values(gamut_df, node):
    """find the top 10 most used values for each attribute and return as sample_values"""
    
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    all_vals = pd.DataFrame()    

    df = gamut_df

    if df.empty==False:
        df['Count'] = 1
        atts = df['Gamut_Attribute_Name'].unique()
    
        vals = pd.DataFrame(df.groupby(['Gamut_Attribute_Name', 'Gamut_Normalized Value'])['Count'].sum())
        vals = vals.reset_index()
 
        for attribute in atts:
            temp_att = vals.loc[vals['Gamut_Attribute_Name']== attribute]

            #pull the top 10 values and put into 'sample' field
            temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(10)
            top_vals = pd.concat([top_vals, temp_att], axis=0)

            #put all attribute values into a single string for TF-IDF processing later            
            temp_df = df.loc[df['Gamut_Attribute_Name']== attribute]
            temp_df['Gamut ALL Values'] = ' '.join(item for item in temp_df['Gamut_Normalized Value'] if item)

            all_vals= pd.concat([all_vals, temp_df], axis=0)
                        
        top_vals = top_vals.groupby('Gamut_Attribute_Name')['Gamut_Normalized Value'].apply('; '.join).reset_index()
        
        all_vals = all_vals.drop_duplicates(subset='Gamut_Attr_ID')
        all_vals = all_vals[['Gamut_Attr_ID', 'Gamut ALL Values']]
    else:
        print('Gamut node {} NO VALUES'.format(node))
        
    return all_vals, top_vals


def get_ws_values(df):
    """find the top 10 most used values for each attribute and return as sample_values"""
    
    top_vals = pd.DataFrame()
    temp_att = pd.DataFrame()
    all_vals = pd.DataFrame()
    browsable_skus = pd.DataFrame()
    
    df['Count'] =1
    atts = df['WS_Attribute_Name'].unique()
    

    vals = pd.DataFrame(df.groupby(['WS_Attribute_Name', 'WS_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()

    for attribute in atts:
        temp_att = vals.loc[vals['WS_Attribute_Name']== attribute]
        
        #pull the top 10 values and put into 'sample' field
        temp_att = temp_att.sort_values(by=['Count'], ascending=[False]).head(10)
        top_vals = pd.concat([top_vals, temp_att], axis=0)
        
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['WS_Attribute_Name']== attribute]
        temp_df['WS ALL Values'] = ' '.join(item for item in temp_df['WS_Attribute_Value'] if item)
        all_vals= pd.concat([all_vals, temp_df], axis=0)

    top_vals = top_vals.groupby('WS_Attribute_Name')['WS_Attribute_Value'].apply('; '.join).reset_index()
    all_vals = all_vals.drop_duplicates(subset='WS_Attr_ID')
    all_vals = all_vals[['WS_Attr_ID', 'WS ALL Values']]

    browsable_skus = df
    pmCode = ['R4', 'R9']
    salesCode = ['DG', 'DV', 'WG', 'WV']
    browsable_skus = browsable_skus[~browsable_skus.PM_Code.isin(pmCode)]
    browsable_skus = browsable_skus[~browsable_skus.Sales_Status.isin(salesCode)]

    total = browsable_skus['WS_SKU'].nunique()
    
    if total > 0:
        browsable_skus = browsable_skus.drop_duplicates(subset=['WS_SKU', 'WS_Attribute_Name'])  #create list of unique grainger skus that feed into gamut query

        browsable_skus['WS_Fill_Rate_%'] = (browsable_skus.groupby('WS_Attribute_Name')['WS_Attribute_Name'].transform('count')/total)*100
        browsable_skus['WS_Fill_Rate_%'] = browsable_skus['WS_Fill_Rate_%'].map('{:,.2f}'.format)
    
        fill_rate = pd.DataFrame(browsable_skus.groupby(['WS_Attribute_Name'])['WS_Fill_Rate_%'].count()/total*100).reset_index()
        fill_rate = fill_rate.sort_values(by=['WS_Fill_Rate_%'], ascending=False)
        
        browsable_skus = browsable_skus[['WS_Attribute_Name']].drop_duplicates(subset='WS_Attribute_Name')
        fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['WS_Attribute_Name'])
        fill_rate['WS_Fill_Rate_%'] = fill_rate['WS_Fill_Rate_%'].map('{:,.2f}'.format)  
        
    else:
        df['WS_Fill_Rate_%'] = 'no browsable SKUs'
        fill_rate = df[['WS_Attribute_Name']].drop_duplicates(subset='WS_Attribute_Name')
        fill_rate['WS_Fill_Rate_%'] = 'no browsable SKUs'

    return all_vals, top_vals, fill_rate


def gamut_process(node, Gamut_allCATS_df, gamut_dict: Dict, k):
    """if gamut node has not been previously processed (in gamut_dict), process and add it to the dictionary"""
    
    gamut_sample_vals = pd.DataFrame()
    gamut_att_vals = pd.DataFrame()

    gamut_df_1 = Gamut_allCATS_df.loc[Gamut_allCATS_df['Gamut_Node_ID']== node] #get gamut attribute values for each gamut_l3 node

    if gamut_df_1.empty==False:
        gamut_df_2 = gamut.gamut_q(gamut_usage_query, 'tax_att."categoryId"', node)
        print('Gamut ', node)
        
        if gamut_df_2.empty==False:
            gamut_df_2 = gamut_df_2.groupby(['Gamut_Attr_ID'])['Gamut_MERCH_Usage'].apply('; '.join).reset_index()

            gamut_df = pd.merge(gamut_df_1, gamut_df_2, how = 'outer', on = 'Gamut_Attr_ID')

        else:
            gamut_df = gamut_df_1
            gamut_df['Gamut_MERCH_Usage'] = ""
            
        gamut_df.loc[gamut_df['Gamut_MERCH_Usage'] == '', 'Gamut_MERCH_Usage'] = np.nan
        gamut_df = gamut_df.sort_values(['Gamut_Attr_ID', 'Gamut_MERCH_Usage']).drop_duplicates(subset = 'Gamut_Attr_ID')
        
        gamut_att_vals, gamut_sample_vals = gamut_values(gamut_df, node) #gamut_values exports a list of --all-- normalized values and sample_values
        
        if gamut_att_vals.empty==False:
            gamut_sample_vals = gamut_sample_vals.rename(columns={'Gamut_Normalized Value': 'Gamut Attribute Sample Values'})

            gamut_df = pd.merge(gamut_df, gamut_sample_vals, on=['Gamut_Attribute_Name'])  #add top 10 normalized values to report
            gamut_df = pd.merge(gamut_df, gamut_att_vals, on=['Gamut_Attr_ID'])

        gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
        
        gamut_dict[node] = gamut_df #store the processed df in dict for future reference
 
    else:
        print('{} EMPTY DATAFRAME'.format(node))    
        
    return gamut_dict, gamut_df


def ws_process(ws_df, ws_sample, ws_all, fill_rate, Gamut_allCATS_df, gamut_dict: Dict, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    
    df = pd.DataFrame()
    gamut_skus = pd.DataFrame()
    
    node_name = ws_df['WS_Node_Name'].unique().tolist()
    node_name = cat_name.pop()
    print('node name = {} {}'.format(k, node_name))

#    ws_skus = ws_df.drop_duplicates(subset='WS_SKU')  #create list of unique WS skus that feed into gamut query
    ws_skus = ws_df['WS_SKU'].unique().tolist()
    print('ws sku count = ', len(ws_skus))

    ws_df = ws_df.drop_duplicates(subset=['WS_Category_ID', 'WS_Attr_ID'])  #group by WS_Node_ID and attribute name and keep unique

    ws_df = ws_df.drop(['WS_SKU', 'WS_Attribute_Value'], axis=1) #remove unneeded columns
    
    ws_df = pd.merge(ws_df, ws_sample, on=['WS_Attribute_Name'])
    ws_df = pd.merge(ws_df, ws_all, on=['WS_Attr_ID'])
    ws_df = pd.merge(ws_df, fill_rate, on=['WS_Attribute_Name'])
    
    ws_df['alt_ws_name'] = process.process_att(ws_df['WS_Attribute_Name'])  #prep att name for merge

    gamut_skus = Gamut_allCATS_df[Gamut_allCATS_df['Gamut_SKU'].isin(ws_skus)]
    
    if gamut_skus.empty==False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node 
        gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        print('GWS L3s ', gamut_l3)
        
        for node in gamut_l3:
            if node in gamut_dict:
                gamut_df = gamut_dict[node]
                print ('node {} in gamut dict'.format(node))

            else:
                gamut_dict, gamut_df = gamut_process(node, Gamut_allCATS_df, gamut_dict, k)




*******START HERE***********************8
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
                temp_df['grainger_sku_count'] = grainger_sku_count
                temp_df['gamut_sku_count'] = len(skus)
                temp_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '+node_name
                temp_df['Gamut/Grainger SKU Counts'] = temp_df['gamut_sku_count'].map(str)+' / '+temp_df['grainger_sku_count'].map(str)

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')

            else:
                print('GWS Node {} EMPTY DATAFRAME'.format(node))
    else:
        grainger_df['Gamut/Grainger SKU Counts'] = '0 / '+str(grainger_sku_count)
        grainger_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '
        df = grainger_df
        print('No Gamut SKUs for Grainger node {}'.format(k))
        
    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes














ws_df = pd.DataFrame()
gamut_df = pd.DataFrame()
attribute_df = pd.DataFrame()
gamut_dict = dict()




# read in WS data
print('Choose WS PIM file')
allCATS_df = q.get_att_values()

# read in and clean WS data
print('\nChoose Gamut file')
Gamut_allCATS_df = q.get_att_values()

print('working...')
start_time = time.time()

node_ids = allCATS_df['WS_Node_ID'].unique().tolist()
print('number of nodes = ', len(node_ids))


for k in node_ids:
    ws_df = allCATS_df.loc[allCATS_df['WS_Node_ID']== k]
    
    if temp_df == False:
        ws_att_vals, ws_sample_vals, ws_fill_rates = get_ws_values(ws_df)
        ws_sample_vals = ws_sample_vals.rename(columns={'WS_Attribute_Value': 'WS Attribute Sample Values'})
        ws_att_vals = ws_att_vals.rename(columns={'WS_Attribute_Value': 'WS ALL Values'})

        temp_df, gamut_dict = ws_process(ws_df, ws_sample_vals, ws_att_vals, ws_fill_rates, Gamut_allCATS_df, gamut_dict, k)
        attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
        print ('WS node = ', k)
    
        else:
        print('No attribute data')
