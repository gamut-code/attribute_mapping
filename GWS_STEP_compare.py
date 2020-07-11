# -*- coding: utf-8 -*-
"""
Created on Tues June 30 2020
@author: xcxg109
"""


""" FLIP between GWS and sandbox"""
#from GWS_query import GWSQuery
from GWS_TOOLBOX_query import GWS_TOOLBOX_Query
""""""

from grainger_query import GraingerQuery
from queries_PIM import gamut_hier_query
import pandas as pd
import file_data as fd
import settings
import time


gcom = GraingerQuery()

""" FLIP between GWS and sandbox"""
#gamut = GWSQuery()
gamut = GWS_TOOLBOX_Query()
""" FLIP between GWS and sandbox"""




#variation of the basic query designed to include discontinued items
grainger_discontinued_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.PM_CODE
            , item.SALES_STATUS
            
            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        	--	AND item.DELETED_FLAG = 'N'
                
            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV')
                AND {} IN ({})
            """




def gamut_data(grainger_df):
       sku_list = grainger_df['Grainger_SKU'].tolist()
       gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
       gamut_df = gamut.gamut_q(gamut_short_query, 'tprod."supplierSku"', gamut_skus)
       return gamut_df
    
#determine SKU or node search
data_type = fd.search_type()
search_level = 'cat.CATEGORY_ID'
gamut_df = pd.DataFrame()

if data_type == 'node':
    search_level = fd.blue_search_level()
elif data_type == 'value' or data_type == 'name':
    while True:
        try:
            val_type = input('Search Type?:\n1. Exact value \n2. Value contained in field ')
            if val_type in ['1', 'EXACT', 'exact', 'Exact']:
                val_type = 'exact'
                break
            elif val_type in ['2', '%']:
                val_type = 'approx'
                break
        except ValueError:
            print('Invalid search type')

search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')

if data_type == 'node':
    for k in search_data:
        grainger_df = gcom.grainger_q(grainger_short_query, search_level, k)
        if grainger_df.empty == False:
            gamut_df = gamut_data(grainger_df)
            fd.shorties_data_out(settings.directory_name, grainger_df, gamut_df, search_level)
        else:
           print('All SKUs are R4, R9, or discontinued')
        print(k)
      
elif data_type == 'sku':
    search_level = 'SKU'
    sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
    grainger_df = gcom.grainger_q(grainger_short_query, 'item.MATERIAL_NO', sku_str)
    gamut_df = gamut_data(grainger_df)
    fd.shorties_data_out(settings.directory_name, grainger_df, gamut_df, search_level)