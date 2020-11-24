# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import settings_NUMERIC as settings
import pandas as pd
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd
from queries_WS import grainger_basic_query, gws_hier_query, STEP_ETL_query
import WS_query_code as q
import time

gws = GWSQuery()
gcom = GraingerQuery()


gws_basic_query="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "WS_PIM_Path"
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY

    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***
"""

grainger_discontinued_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SUPPLIER_NO
            , item.PM_CODE
            , item.SALES_STATUS
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PRICING_FLAG
            , item.PRICER_FIRST_EFFECTIVE_DATE

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
         		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')
                
            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                 AND item.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)
                 AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND {} IN ({})
            """
            
            
#get basic SKU list and hierarchy data from Grainger teradata material universe
category_query="""
            SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                                
            WHERE {} IN ({})
            """
            
def gws_data(grainger_df, lookup_df):
    gws_data = pd.DataFrame()    

    gws_seg = grainger_df['Segment_Name'].unique().tolist()

    for k in gws_seg:
        print('pulling SKUs for ', k)
        
        l1_df = lookup_df.loc[lookup_df['Segment_Name']== k]
        l1_id = l1_df['GWS_L1'].unique()
        
        nodes_df = gws.gws_q(gws_basic_query, 'tprod."categoryId"', l1_id[0])           
        
        if nodes_df.empty == False:
            node_ids = nodes_df['WS_Node_ID'].unique().tolist()
            print('number of nodes = ', len(node_ids))

            for j in node_ids:
                temp_gws_df = gws.gws_q(gws_hier_query, 'tprod."categoryId"', j)    

                gws_data = pd.concat([gws_data, temp_gws_df], axis=0)
            
    return gws_data


def df_merge(gr_df, gw_df):
    gr_df = gr_df.merge(gw_df, how="left", on=["Grainger_SKU"])
    
    columnsTitles = ['Grainger_SKU', 'WS_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                     'Category_ID', 'Category_Name', 'SUPPLIER_NO', 'RELATIONSHIP_MANAGER_CODE', 'PM_CODE', \
                     'SALES_STATUS', 'PRICING_FLAG', 'PRICER_FIRST_EFFECTIVE_DATE', 'GWS_Category_ID', \
                     'GWS_Category_Name', 'GWS_Node_ID', 'GWS_Node_Name']

    #gr_df.to_csv('C:/Users/xcxg109/NonDriveFiles/gr_test.csv')
    
    gr_df = gr_df.reindex(columns=columnsTitles)
    
    return gr_df


#determine whether or not to include discontinued items in the data pull
def skus_to_pull():
    """choose whether to included discontinued SKUs"""
    while True:
        try:
            sku_status = input("Include DISCOUNTINUED skus? ")
            if sku_status in ['Y', 'y', 'Yes', 'YES', 'yes']:
                sku_status = 'all'
                break
            elif sku_status in ['N', 'n', 'No', 'NO', 'no']:
                sku_status = 'filtered'
                break
        except ValueError:
            print('Invalid search type')
        
    return sku_status


gws_df = pd.DataFrame()
grainger_df = pd.DataFrame()

# read in grainger data
lookup_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/code/L1_lookup_table.csv')

quer = 'HIER'
gws_stat = 'no'
search_level = 'cat.SEGMENT_ID'
data_type = 'grainger_query'

sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull        
search_data = fd.data_in(data_type, settings.directory_name)

            
start_time = time.time()
print('working...')

for k in search_data:
    print('K = ', k)
    
    if sku_status == 'filtered':
#            grainger_df = gcom.grainger_q(grainger_basic_query, search_level, k)
        temp_df = gcom.grainger_q(STEP_ETL_query, search_level, k)
        
    elif sku_status == 'all':
        temp_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
        
    if temp_df.empty == False:
#            grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/test_hier.csv')
        gws_df = gws_data(temp_df, lookup_df)            

        if gws_df.empty == False:
            gws_stat = 'yes'
            temp_df = df_merge(temp_df, gws_df)

        temp_df = temp_df.loc[temp_df['WS_SKU'].isnull()]
        
        grainger_df = pd.concat([grainger_df, temp_df], axis=0)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

fd.hier_data_out(settings.directory_name, grainger_df, quer, gws_stat, search_level)
        

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))