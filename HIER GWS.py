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
from queries_WS import grainger_basic_query, gws_hier_query
import WS_query_code as q
import time

gws = GWSQuery()
""" """
gcom = GraingerQuery()


def gws_data(grainger_df):
    sku_list = grainger_df['Grainger_SKU'].tolist()
    gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
    gws_df = gws.gws_q(gws_hier_query, 'tprod."gtPartNumber"', gws_skus)

    return gws_df


def grainger_data(gws_df):

    sku_list = gws_df['supplierProductId'].tolist()
    """ """

    grainger_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', grainger_skus)
    return grainger_df


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
        except ValueeError:
            print('Invalid search type')
        
    return sku_status


gws_df = pd.DataFrame()
grainger_df = pd.DataFrame()


quer = 'HIER'
search_level = 'tax.id'

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()
    sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull

search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')


if data_type == 'gamut_query':
    for k in search_data:
        temp_df = gws.gws_q(gws_hier_query, search_level, k)

        gws_df = pd.concat([gws_df, temp_df], axis=0)
        if gws_df.empty == False:
            grainger_df = grainger_data(gws_df)
            fd.hier_data_out(settings.directory_name, grainger_df, gws_df, quer, search_level)
        else:
            print('{} No SKUs in node'.format(k))

elif data_type == 'grainger_query':
    for k in search_data:
        if sku_status == 'filtered':
            temp_df = gcom.grainger_q(grainger_basic_query, search_level, k)
        elif sku_status == 'all':
            temp_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
        grainger_df = pd.concat([grainger_df, temp_df], axis=0)
        if grainger_df.empty == False:
            gws_df = gws_data(grainger_df)
            fd.hier_data_out(settings.directory_name, grainger_df, gws_df, quer, search_level)
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

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
