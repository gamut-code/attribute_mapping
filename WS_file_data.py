# -*- coding: utf-8 -*-
"""
Created on Fri Feb 12 15:28:30 2021

@author: xcxg109

basic input / export file used by other files to gather user data and create file oupts
"""

from pathlib import Path
import pandas as pd
import settings_NUMERIC as settings
import pandas.io.formats.excel
import os
import string


def WS_search_type():
    """ choose data type to import for WS only reports """
    while True:
        try:
            data_type = input('Search by: \n1. WS category\n2. WS node\n3. SKU ')
            if data_type in ['1']:
                data_type = 'category'
                break
            elif data_type in ['2']:
                data_type = 'node'
                break
            elif data_type in ['3']:
                data_type = 'sku'
                break
        except ValueError:
            print('Invalid search type')

    return data_type


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
#    type_list = ['Node', 'SKU']
    
    if data_type == 'category':
        search_data = input ('Input WS category ID or ENTER to read from file: ')
    if data_type == 'node':
        search_data = input ('Input WS terminal node ID or ENTER to read from file: ')        
    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')
    elif data_type == 'supplier':
        search_data = input ('Input Supplier ID to search for: ')
        
    if search_data != "":
        search_data = search_data.strip()
        search_data = [search_data]
    else:
        file_data = settings.get_file_data()

        if data_type == 'sku':
            search_data = [row[0] for row in file_data[1:]]
        else:
            search_data = [int(row[0]) for row in file_data[1:]]

    return search_data


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]
