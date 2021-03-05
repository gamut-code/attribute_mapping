# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_WS import grainger_hier_query, grainger_discontinued_query, ws_hier_query
import file_data_GWS as fd
import pandas as pd
import settings_NUMERIC as settings
import time


gcom = GraingerQuery()
gws = GWSQuery()


def gws_data(grainger_df):
    gws_sku_list = pd.DataFrame()
    
    sku_list = grainger_df['STEP_SKU'].tolist()
    
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
            temp_df = gws.gws_q(ws_hier_query, 'tprod."gtPartNumber"', gws_skus)
            
            gws_sku_list = pd.concat([gws_sku_list, temp_df], axis=0, sort=False) 
            
    else:
        gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
        gws_sku_list = gws.gws_q(ws_hier_query, 'tprod."gtPartNumber"', gws_skus)

    return gws_sku_list


def grainger_data(gws_df, sku_status):
    
    sku_list = gws_df['WS_SKU'].tolist()
    grainger_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
    if sku_status == 'filtered':
        grainger_df = gcom.grainger_q(grainger_hier_query, 'item.MATERIAL_NO', grainger_skus )
                
    elif sku_status == 'all':
        grainger_df = gcom.grainger_q(grainger_discontinued_query, 'item.MATERIAL_NO', grainger_skus )

    return grainger_df


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger Blue \n2. Grainger Yellow \n3. GWS \n4. SKU ")
            if data_type in ['1']:
                data_type = 'grainger_query'
                break
            if data_type in ['2']:
                data_type = 'yellow'
                break
            elif data_type in ['3']:
                data_type = 'gws_query'
                break
            elif data_type in ['4']:
                data_type = 'sku'
                break
        except ValueError:
            print('Invalid search type')
        
    return data_type


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


print('working....')
quer='HIER'
grainger_df = pd.DataFrame()

#request the type of data to pull: blue or yellow, SKUs or node, single entry or read from file
data_type = search_type()
search_level = 'cat.CATEGORY_ID'

#if Blue is chosen, determine the level to pull L1 (segment), L2 (family), or L1 (category)
if data_type == 'grainger_query':
    search_level = fd.blue_search_level()

#ask user for node number/SKU or pull from file if desired    
search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull

grainger_df = pd.DataFrame()

print('working....')

if data_type == 'grainger_query':
    for k in search_data:
        if sku_status == 'filtered':
            temp_df = gcom.grainger_q(grainger_hier_query, search_level, k)

        elif sku_status == 'all':
            temp_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
        
        if temp_df.empty == False:
            gws_df = gws_data(temp_df)

            if gws_df.empty == False:
                gws_stat = 'yes'
                temp_df = temp_df.merge(gws_df, how="left", left_on="STEP_SKU", right_on='WS_SKU')

            grainger_df = pd.concat([grainger_df, temp_df], axis=0)
            print(k)
        
elif data_type == 'yellow':
    for k in search_data:
        if isinstance(k, int):#k.isdigit() == True:
            pass
        else:
            k = "'" + str(k) + "'"
            
        if sku_status == 'filtered':
            temp_df = gcom.grainger_q(grainger_hier_query, 'yellow.PROD_CLASS_ID', k)

        elif sku_status == 'all':
            temp_df = gcom.grainger_q(grainger_discontinued_query, 'yellow.PROD_CLASS_ID', k)
            
        if temp_df.empty == False:
            gws_df = gws_data(temp_df)

            if gws_df.empty == False:
                gws_stat = 'yes'
                temp_df = temp_df.merge(gws_df, how="left", left_on="STEP_SKU", right_on='WS_SKU')

            grainger_df = pd.concat([grainger_df, temp_df], axis=0)            
            print(k)
        
elif data_type == 'sku':    
    search_level = 'SKU'

    if len(search_data)>4000:
        num_lists = round(len(search_data)/4000, 0)
        num_lists = int(num_lists)

        if num_lists == 1:
            num_lists = 2

        print('running GWS SKUs in {} batches'.format(num_lists))

        size = round(len(search_data)/num_lists, 0)
        size = int(size)

        div_lists = [search_data[i * size:(i + 1) * size] for i in range((len(search_data) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            print('batch {} of {}'.format(k+1, num_lists))
            sku_str  = ", ".join("'" + str(i) + "'" for i in div_lists[k])

            if sku_status == 'filtered':
                temp_df = gcom.grainger_q(grainger_hier_query, 'item.MATERIAL_NO', sku_str)

            elif sku_status == 'all':
                temp_df = gcom.grainger_q(grainger_discontinued_query, 'item.MATERIAL_NO', sku_str)

            grainger_df = pd.concat([grainger_df, temp_df], axis=0, sort=False)

    else:
        sku_str  = ", ".join("'" + str(i) + "'" for i in search_data)
        
        if sku_status == 'filtered':
            grainger_df = gcom.grainger_q(grainger_hier_query, 'item.MATERIAL_NO', sku_str)

        elif sku_status == 'all':
            grainger_df = gcom.grainger_q(grainger_discontinued_query, 'item.MATERIAL_NO', sku_str)
            
    if grainger_df.empty == False:
        gws_df = gws_data(grainger_df)

        if gws_df.empty == False:
            gws_stat = 'yes'
            grainger_df = grainger_df.merge(gws_df, how="left", left_on="STEP_SKU", right_on='WS_SKU')

elif data_type == 'gws_query':
    gws_stat = 'yes'
    
    for k in search_data:
        temp_df = gws.gws_q(ws_hier_query, 'tprod."categoryId"', k)
        
        if temp_df.empty == False:
            grainger_skus_df = grainger_data(temp_df, sku_status)

            if grainger_skus_df.empty == False:
                temp_df = temp_df.merge(grainger_skus_df, how="left", left_on="STEP_SKU", right_on='WS_SKU')

        grainger_df = pd.concat([grainger_df, temp_df], axis=0)            
        print(k)

fd.hier_data_out(settings.directory_name, grainger_df, quer, gws_stat, search_level)
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
