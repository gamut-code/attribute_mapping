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
from queries_WS import grainger_basic_query, gws_hier_query, grainger_discontinued_query, STEP_ETL_query
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
            
def gws_data(grainger_df):
    gws_data = pd.DataFrame()    
    sku_list = grainger_df['Grainger_SKU'].unique().tolist()
    print('SKUs = ', len(sku_list))

    gws_seg = grainger_df['Segment_Name'].unique().tolist()
    if len(sku_list)>7000:
        num_lists = round(len(sku_list)/7000, 0)
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

            temp_gws_df = gws.gws_q(gws_hier_query, 'tprod."gtPartNumber"', gws_skus)
            gws_data = pd.concat([gws_data, temp_gws_df], axis=0, sort=False)
    
    else:
        gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
        gws_data = gws.gws_q(gws_hier_query, 'tprod."gtPartNumber"', gws_skus)

    return gws_data


def df_merge(gr_df, gw_df):
    gr_df = gr_df.merge(gw_df, how="left", on=["Grainger_SKU"])
    
    columnsTitles = ['Grainger_SKU', 'WS_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                     'Category_ID', 'Category_Name', 'PM_CODE', 'SALES_STATUS', 'GWS_Category_ID', \
                     'GWS_Category_Name', 'GWS_Node_ID', 'GWS_Node_Name']
    
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


quer = 'HIER'
gws_stat = 'no'
search_level = 'tax.id'

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()
    sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull

elif data_type == 'gws_query':
    while True:
        try:
            search_level = input("Search by: \n1. Node Group \n2. Single Category \n3. SKU ")
            if search_level in ['1', 'g', 'G']:
                search_level = 'group'
                break
            elif search_level in ['2', 's', 'S']:
                search_level = 'single'
                break
            elif search_level in ['3', 'sku', 'SKU']:
                search_level = 'sku'
                break
        except ValueError:
            print('Invalid search type')
            
search_data = fd.data_in(data_type, settings.directory_name)
            
start_time = time.time()
print('working...')


if data_type == 'gws_query':
    gws_stat = 'yes'
      
    if search_level == 'single':    
        for k in search_data:
            gws_df = gws.gws_q(gws_hier_query, 'tprod."categoryId"', k)
    
            if gws_df.empty == False:
                fd.hier_data_out(settings.directory_name, gws_df, quer, gws_stat, search_level)

            else:
                print('{} No SKUs in node'.format(k))

    elif search_level == 'group':
        for node in search_data:
            df = gws.gws_q(gws_basic_query, 'tprod."categoryId"', node)           

            print('k = ', node)

            if df.empty == False:
                node_ids = df['WS_Node_ID'].unique().tolist()
                print('number of nodes = ', len(node_ids))

            for j in node_ids:
                print(j)
                temp_df = gws.gws_q(gws_hier_query, 'tprod."categoryId"', j)    

                gws_df = pd.concat([gws_df, temp_df], axis=0)
                
            gws_df['Count'] = 1
                
            gws_df = pd.DataFrame(gws_df.groupby(['GWS_Category_ID','GWS_Category_Name','GWS_Node_ID', \
                                                      'GWS_Node_Name','WS_SKU','STEP_Category_ID'])['Count'].sum())

            gws_df = gws_df.reset_index()
            gws_df = gws_df.drop(['Count'], axis=1)

            fd.hier_data_out(settings.directory_name, gws_df, quer, gws_stat, search_level)  
        
elif data_type == 'grainger_query':
    for k in search_data:
        print ('K = ', k)
        if sku_status == 'filtered':
#            grainger_df = gcom.grainger_q(grainger_basic_query, search_level, k)
            grainger_df = gcom.grainger_q(STEP_ETL_query, search_level, k)
            
        elif sku_status == 'all':
            grainger_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
            
        if grainger_df.empty == False:
#            grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/test_hier.csv')
            gws_df = gws_data(grainger_df)            

            if gws_df.empty == False:
                gws_stat = 'yes'
                grainger_df = df_merge(grainger_df, gws_df)

            fd.hier_data_out(settings.directory_name, grainger_df, quer, gws_stat, search_level)
            print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

        else:
           print('All SKUs are R4, R9, or discontinued')       
           
elif data_type == 'sku':
    search_level = 'SKU'
    sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', sku_str)
    if grainger_df.empty == False:
        gws_df = gws_data(grainger_df)    
        if gws_df.empty == False:
            gamut = 'yes'
            grainger_df = grainger_df.merge(gws_df, how="left", on=["Grainger_SKU"])
            fd.data_out(settings.directory_name, grainger_df, quer, search_level)
    else:
        print('No SKU data for ', sku_str)
