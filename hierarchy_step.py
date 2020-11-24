1# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

from grainger_query import GraingerQuery
from queries_WS import grainger_basic_query, grainger_discontinued_query
import file_data_GWS as fd
import pandas as pd
import settings
import time


STEP_ETL_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        --		AND item.DELETED_FLAG = 'N'
            
            LEFT OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                ON prod.MATERIAL = item.MATERIAL_NO
                
            LEFT OUTER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO
                AND supplier.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND {} IN ({})
            """


#determine whether or not to include discontinued items in the data pull
def skus_to_pull():
    """choose whether to included discontinued SKUs"""
    sku_status = input("Include DISCOUNTINUED skus? ")
    if sku_status in ['Y', 'y', 'Yes', 'YES', 'yes']:
        sku_status = 'all'
    elif sku_status in ['N', 'n', 'No', 'NO', 'no']:
        sku_status = 'filtered'
    else:
        raise ValueError('Invalid search type')
    return sku_status


def generate_data():
    gcom = GraingerQuery()

    #request the type of data to pull: blue or yellow, SKUs or node, single entry or read from file
    data_type = fd.search_type()
    search_level = 'cat.CATEGORY_ID'
    #if Blue is chosen, determine the level to pull L1 (segment), L2 (family), or L1 (category)
    if data_type == 'node':
        search_level = fd.blue_search_level()

    #ask user for node number/SKU or pull from file if desired    
    search_data = fd.data_in(data_type, settings.directory_name)

    sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull

    grainger_df = pd.DataFrame()
    
    if data_type == 'node':
        for k in search_data:
            if sku_status == 'filtered':
                temp_df = gcom.grainger_q(grainger_basic_query, search_level, k)
                temp_df = gcom.grainger_q(grainger_basic_query, search_level, k)
                
            elif sku_status == 'all':
                temp_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
                
            grainger_df = pd.concat([grainger_df, temp_df], axis=0)
            print(k)
            
    elif data_type == 'yellow':
        for k in search_data:
            if isinstance(k, int):#k.isdigit() == True:
                pass
            else:
                k = "'" + str(k) + "'"
                
            if sku_status == 'filtered':
                temp_df = gcom.grainger_q(grainger_basic_query, 'yellow.PROD_CLASS_ID', k)
            elif sku_status == 'all':
                temp_df = gcom.grainger_q(grainger_discontinued_query, 'yellow.PROD_CLASS_ID', k)
                
            grainger_df = pd.concat([grainger_df, temp_df], axis=0)            
            print(k)
            
    elif data_type == 'sku':    
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
        
        if sku_status == 'filtered':
                grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', sku_str)
        elif sku_status == 'all':
                grainger_df = gcom.grainger_q(grainger_discontinued_query, 'item.MATERIAL_NO', sku_str)
                
    elif data_type == 'supplier':
        for k in search_data:
            if sku_status == 'filtered':
                temp_df = gcom.grainger_q(grainger_basic_query, 'supplier.SUPPLIER_NO', k)
            elif sku_status == 'all':
                temp_df = gcom.grainger_q(grainger_discontinued_query, 'supplier.SUPPLIER_NO', k)
            
            grainger_df = pd.concat([grainger_df, temp_df], axis=0)            
            print(k)
            
    return [grainger_df, search_level]