# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import re
from grainger_query import GraingerQuery
from GWS_query import GWSQuery
from queries_WS import grainger_attr_query, grainger_value_query, gws_attr_values
import file_data_GWS as fd
import settings_NUMERIC as settings
import time

gws = GWSQuery()


def get_stats(df):
    """return unique values for each attribute with a count of how many times each is used in the node"""
    df['Count'] =1
    stats = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    return stats


def item_search(analysis, searchfor):
    """search the dictionary of attributes for any key containing the passed in value. Used to look for any 'Item' attributes"""
    total = [value for (key, value) in analysis.items() if searchfor in key]
    if len(total) > 1:
        total = max(total)
    return total


def get_fill_rate(df):
    browsable_skus = pd.DataFrame()

    # eliminate all discontinueds and R4/R9 before calculating fill rate
    browsable_skus = df
    pmCode = ['R4', 'R9']
    salesCode = ['DG', 'DV', 'WG', 'WV']
    browsable_skus = browsable_skus[~browsable_skus.PM_Code.isin(pmCode)]
    browsable_skus = browsable_skus[~browsable_skus.Sales_Status.isin(salesCode)]

    total = browsable_skus['Grainger_SKU'].nunique()

    if total > 0:
        browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Grainger_Attribute_Name'])  #create list of unique grainger skus that feed into gamut query

        browsable_skus['Grainger_Fill_Rate_%'] = (browsable_skus.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Name'].transform('count')/total)*100
        browsable_skus['Grainger_Fill_Rate_%'] = browsable_skus['Grainger_Fill_Rate_%'].map('{:,.2f}'.format)
    
        fill_rate = pd.DataFrame(browsable_skus.groupby(['Grainger_Attribute_Name'])['Grainger_Fill_Rate_%'].count()/total*100).reset_index()
        fill_rate = fill_rate.sort_values(by=['Grainger_Fill_Rate_%'], ascending=False)

        browsable_skus = browsable_skus[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Grainger_Attribute_Name'])
        fill_rate['Grainger_Fill_Rate_%'] = fill_rate['Grainger_Fill_Rate_%'].map('{:,.2f}'.format)  

    else:
        df['Grainger_Fill_Rate_%'] = 'no browsable SKUs'
        fill_rate = df[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate['Grainger_Fill_Rate_%'] = 'no browsable SKUs'

    return fill_rate


def gws_values(df):
    df['WS_Value'] = ''
    
    for row in df.itertuples():
        dt = df.at[row.Index, 'Data_Type']
        val = df.at[row.Index, 'Normalized_Value']
        
        if dt == 'number':
            unit = df.at[row.Index, 'Normalized_Unit']
            
            ws_val = str(val) + ' ' + str(unit)
            df.at[row.Index, 'WS_Value'] = ws_val
            
        else:
            df.at[row.Index, 'WS_Value'] = val
                    
    return df


#determine SKU or node search
ws_df = pd.DataFrame()
data_type = 'gws_query'

while True:
    try:
        search_level = input("Search by: \n1. Node Group \n2. Single Category \n3. SKU \n4. Other ")
        if data_type in ['1', 'g', 'G']:
            search_level = 'group'
            break
        elif search_level in ['2', 's', 'S']:
            search_level = 'single'
            break
        elif search_level in ['3', 'sku', 'Sku', 'SKU', 's', 'S']:
                data_type = 'sku'
                break
        elif search_level in ['4', 'other', 'Other', 'OTHER', 'o', 'O']:
            search_level = 'other'
            break
    except ValueError:
        print('Invalid search type')
        
if search_level == 'other':
    while True:
        try:
            data_type = input ('Query Type?\n1. Attribute Value\n2. Attribute Name\n3. UOM Group \n4. UOM Value ')
            if data_type in ['attribute value', 'Attribute Value', 'value', 'Value', 'VALUE', '1']:
                data_type = 'value'
                break
            elif data_type in ['attribute name', 'Attribute Name', 'name', 'Name', 'NAME', '2']:
                data_type = 'name'
                break
            elif data_type in ['3']:
                data_type = 'uom_group'
                break
            elif data_type in ['4']:
                data_type = 'uom_val'
                break
        except ValueError:
            print('Invalid search type')

search_data = fd.data_in(data_type, settings.directory_name)

if data_type == 'value' or data_type == 'name' or data_type == 'uom_val':
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
    
start_time = time.time()
print('working...')
        

if data_type == 'sku':
        val_type = '_regular'
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)

#        df = gcom.grainger_q(grainger_attr_query, 'item.MATERIAL_NO', sku_str)
        df = gws.gws_q(gws_attr_values, 'tprod."gtPartNumber"', sku_str)
 
        if df.empty == False:
            search_level = 'SKU'
            df_stats = get_stats(df)
            df_fill = get_fill_rate(df)
            fd.attr_data_out(settings.directory_name, df, df_stats, df_fill, search_level, val_type)

        else:
            print('All SKUs are R4, R9, or discontinued')

        print(search_data)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
        
elif data_type == 'name':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass

            else:
                k = "'" + str(k) + "'"

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"

#        df = gcom.grainger_q(grainger_value_query, 'attr.DESCRIPTOR_NAME', k)
        df = gws_gws_q(gws_attr_values, 'tax_att.name', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, 'ATTRIBUTE Name', search_level)

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'value':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass
 
            else:
                k = "'" + str(k) + "'"

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"

#        df = gcom.grainger_q(grainger_value_query, 'item_attr.ITEM_DESC_VALUE', k)
        df = gws.gws_q(gws_attr_values, 'tprodvalue."valueNormalized"', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, 'ATTRIBUTE Value', search_level)

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'uom_group':
    for k in search_data:
        df = gws.gws_q(gws_attr_values, 'tax_att."unitGroupId"', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, 'UOM Group', search_level)

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'uom_val':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass
 
            else:
                k = "'" + str(k) + "'"

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"

#        df = gcom.grainger_q(grainger_value_query, 'item_attr.ITEM_DESC_VALUE', k)
        df = gws.gws_q(gws_attr_values, 'tprodvalue."unitNormalized"', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, 'UOM Value', search_level)

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
