# -*- coding: utf-8 -*-
"""
Created on Thur Aug 20 2020

@author: xcxg109
"""

from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_WS import ws_short_query, grainger_short_query, grainger_short_values
import pandas as pd
import file_data_GWS as fd
import settings_NUMERIC as settings
import time


gcom = GraingerQuery()
gws = GWSQuery()

def gws_data(grainger_df):
    
    sku_list = grainger_df['WS_SKU'].tolist()
    gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    gws_df = gws.gws_q(gws_short_query, 'tprod."gtPartNumber"', gws_skus)

    return gws_df
    

def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Blue (node)\n2. Yellow\n3. SKU\n4. Other ")
            if data_type in ['1', 'node', 'Node', 'NODE', 'blue', 'Blue', 'BLUE', 'b', 'B']:
                data_type = 'node'
                break
            elif data_type in ['2', 'yellow', 'Yellow', 'YELLOW', 'y', 'Y']:
                data_type = 'yellow'
                break
            elif data_type in ['3', 'sku', 'Sku', 'SKU', 's', 'S']:
                data_type = 'sku'
                break
            elif data_type in ['4', 'other', 'Other', 'OTHER', 'o', 'O']:
                data_type = 'other'
                break
        except ValueError:
            print('Invalid search type')
    
    if data_type == 'other':
        while True:
            try:
                data_type = input ('Query Type?\n1. Attribute Value\n2. Attribute Name\n3. Supplier ID ')
                if data_type in ['attribute value', 'Attribute Value', 'value', 'Value', 'VALUE', '1']:
                    data_type = 'value'
                    break
                elif data_type in ['attribute name', 'Attribute Name', 'name', 'Name', 'NAME', '2']:
                    data_type = 'name'
                    break
                if data_type in ['supplier id', 'supplier ID', 'Supplier ID', 'SUPPLIER ID', 'Supplier id', 'ID', 'id', '3']:
                    data_type = 'supplier'
                    break
            except ValueError:
                print('Invalid search type')
    
    return data_type


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
    
    if data_type == 'node':
        search_data = input('Input Blue node ID or hit ENTER to read from file: ')

    elif data_type == 'yellow':
        search_data = input('Input Yellow node ID or hit ENTER to read from file: ')

    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')

    elif data_type == 'value':
        search_data = input ('Input attribute value to search for: ')

    elif data_type == 'name':
        search_data = input ('Input attribute name to search for: ')

    elif data_type == 'supplier':
        search_data = input ('Input Supplier ID to search for: ')
        
    if search_data != "":
        search_data = [search_data]
        return search_data
    else:
        file_data = settings.get_file_data()

        if data_type == 'node':
            search_data = [int(row[0]) for row in file_data[1:]]
            return search_data

        elif data_type == 'yellow':
            search_data = [str(row[0]) for row in file_data[1:]]
            return search_data

        elif data_type == 'sku':
            search_data = [row[0] for row in file_data[1:]]
            return search_data        


    
#determine SKU or node search
data_type = search_type()

search_level = 'cat.CATEGORY_ID'

gws_df = pd.DataFrame()
grainger_df = pd.DataFrame()

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

search_data = data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')

if data_type == 'node':
    for k in search_data:

        grainger_df = gcom.grainger_q(grainger_short_query, search_level, k)

        if grainger_df.empty == False:
            gws_df = gws_data(grainger_df)
            fd.shorties_data_out(settings.directory_name, grainger_df, gws_df, search_level)

        else:
           print('All SKUs are R4, R9, or discontinued')
        print(k)

elif data_type == 'yellow':
    for k in search_data:
        if isinstance(k, int):#k.isdigit() == True:
           pass

        else:
           k = "'" + str(k) + "'"

        grainger_df = gcom.grainger_q(grainger_short_query, 'yellow.PROD_CLASS_ID', k)

        if grainger_df.empty == False:
           gws_df = gws_data(grainger_df)
           fd.shorties_data_out(settings.directory_name, grainger_df, gws_df, search_level)

        else:
           print('All SKUs are R4, R9, or discontinued')

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
            sku_str = ", ".join("'" + str(i) + "'" for i in div_lists[k])

            temp_df = gcom.grainger_q(grainger_short_query, 'item.MATERIAL_NO', sku_str)

            if temp_df.empty == False:                
                grainger_df = pd.concat([grainger_df, temp_df], axis=0, sort=False)
            else:
                print('Empty dataframe')

    else:
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)

        grainger_df = gcom.grainger_q(grainger_short_query, 'item.MATERIAL_NO', sku_str)

    if grainger_df.empty == False:
        gws_df = gws_data(grainger_df)
    else:
        print('Empty dataframe')
    
    fd.shorties_data_out(settings.directory_name, grainger_df, gws_df, search_level)
    
elif data_type == 'value':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):
                pass      # do nothing if data is numerical

            else:
                k = "'" + str(k) + "'"    # otherwise, package it as a string

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"      # for approximate values, pacage with % wildcards
            
        grainger_df = gcom.grainger_q(grainger_short_values, 'item.GIS_SEO_SHORT_DESC_AUTOGEN', k)
        
        if grainger_df.empty == False:
            gws_df = gws_data(grainger_df)

            fd.shorties_data_out(settings.directory_name, grainger_df, gws_df, search_level)        

        else:
            print('All SKUs are R4, R9, or discontinued')            

        print(k)
        
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
