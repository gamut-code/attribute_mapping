# -*- coding: utf-8 -*-
"""
Created on Tue Oct  1 10:57:44 2019

@author: xcxg109
"""

import file_data_att as fd
import settings
import pandas as pd
from gamut_query_15 import GamutQuery_15
from grainger_query import GraingerQuery
from queries_PIM import gamut_attr_query, grainger_basic_query
import query_code as q

gamut = GamutQuery_15()
gcom = GraingerQuery()


def gamut_data(grainger_df):
       sku_list = grainger_df['Grainger_SKU'].tolist()
       gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
       gamut_df = gamut.gamut_q15(gamut_attr_query, 'tprod."supplierSku"', gamut_skus)
       return gamut_df
       

gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()

search_level = 'tax.id'


data_type = fd.search_type()

search_data = fd.data_in(data_type, settings.directory_name)

if data_type == 'gamut_query':
    for k in search_data:
        temp_df = gamut.gamut_q15(gamut_attr_query, search_level, k)
        gamut_df = pd.concat([gamut_df, temp_df], axis=0)
        if temp_df.empty == True:
            print('{} No SKUs in node'.format(k))
        print(k)
elif data_type == 'sku':
    search_level = 'SKU'
    sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', sku_str)
    gamut_df = gamut_data(grainger_df)
    
    if gamut_df.empty == False:
        gamut = 'yes'
        grainger_df = grainger_df.merge(gamut_df, how="left", on=["Grainger_SKU"])

    fd.data_out(settings.directory_name, grainger_df, search_level)
    
quer = 'ATTR'
fd.data_out(settings.directory_name, gamut_df, quer, search_level)