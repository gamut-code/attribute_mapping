# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import re
from grainger_query import GraingerQuery
from queries_MATCH import grainger_attr_query, grainger_value_query
import file_data_att as fd
import settings
import time

gcom = GraingerQuery()


def get_stats(df):
    """return unique values for each attribute with a count of how many times each is used in the node"""
    df['Count'] =1
    stats = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    return stats


def get_fill_rate(df):
    """find the top 5 most used values for each attribute and return as sample_values"""
    
    browsable_skus = pd.DataFrame()
    
    df['Count'] =1

    browsable_skus = df
    pmCode = ['R4', 'R9']
    salesCode = ['DG', 'DV', 'WG', 'WV']
    browsable_skus = browsable_skus[~browsable_skus.PM_Code.isin(pmCode)]
    browsable_skus = browsable_skus[~browsable_skus.Sales_Status.isin(salesCode)]

    total = browsable_skus['Grainger_SKU'].nunique()
    
    if total > 0:
        browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Grainger_Attribute_Name'])  #create list of unique grainger skus that feed into gamut query

        browsable_skus['Fill_Rate_%'] = (browsable_skus.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Name'].transform('count')/total)*100
        browsable_skus['Fill_Rate_%'] = browsable_skus['Fill_Rate_%'].map('{:,.2f}'.format)
    
        fill_rate = pd.DataFrame(browsable_skus.groupby(['Grainger_Attribute_Name'])['Fill_Rate_%'].count()/total*100).reset_index()
        fill_rate = fill_rate.sort_values(by=['Fill_Rate_%'], ascending=False)
        
        browsable_skus = browsable_skus[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Grainger_Attribute_Name'])
        fill_rate['Fill_Rate_%'] = fill_rate['Fill_Rate_%'].map('{:,.2f}'.format)  
        df = df[['Grainger_Attribute_Name', 'ENDECA_Ranking']]
        df = df.drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate = fill_rate.merge(df, how= "inner", on=['Grainger_Attribute_Name'])
        
    else:
        df['Grainger_Fill_Rate_%'] = 'no browsable SKUs'
        fill_rate = df[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate['Grainger_Fill_Rate_%'] = 'no browsable SKUs'

    return fill_rate
    

def analyze(df):
    all_vals = pd.DataFrame()

    atts = df['Grainger_Attribute_Name'].unique()

    vals = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()

    for attribute in atts:
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df['Num'] = ""
        temp_df['Str'] = ""
        
        for row in temp_df.itertuples():
            value = row.Grainger_Attribute_Value
            
#                match = re.match(r"([0-9]+)([a-z]+)", value, re.I)
#                r = re.compile(r"^\d*[.,]?\d*$")
#                match = re.match(r"^\d*[.,]?\d*$", value, re.I)
#                r = re.compile("^(?=.*?\d)\d*[.,]?\d*$")
#                match = re.match("^(?=.*?\d)\d*[.,]?\d*$", value, re.I)
#            r = re.match("^\d*\.?\d*", value, re.I)

#            r = re.compile('^\d*\.?\d*')
            r = re.compile('^\d*[\.\/]?\d*')

            temp_df.at[row.Index, 'Num'], temp_df.at[row.Index, 'Str'] = re.split(r, value)
            num = r.search(value)           
            temp_df.at[row.Index, 'Num'] = num.group()
            
#            if temp_df['Num'] != "":
#                temp_df['Str'] = row.Grainger_Attribute_Value.extract('^\d*\.?\d*')
   #         if match:
   #             items = match.groups()
   #             temp_df.at[row.Index,'Values'] = items

#        temp_df['Grainger ALL Values'] = ' '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        all_vals= pd.concat([all_vals, temp_df], axis=0)

#    all_vals = all_vals.drop_duplicates(subset='Grainger_Attr_ID')
#    all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values']]

    all_vals.to_csv("F:/CGabriel/Grainger_Shorties/OUTPUT/vals.csv")

    return all_vals

#determine SKU or node search
search_level = 'cat.CATEGORY_ID'
data_type = fd.search_type()


if data_type == 'grainger_query':
    search_level, data_process = fd.blue_search_level()
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

if data_type == 'grainger_query':
    for k in search_data:
        grainger_df = gcom.grainger_q(grainger_attr_query, search_level, k)
        if grainger_df.empty == False:
            df_stats = get_stats(grainger_df)
            df_fill = get_fill_rate(grainger_df)
            all_vals = analyze(grainger_df)
            fd.attr_data_out(settings.directory_name, grainger_df, df_stats, df_fill, search_level)
        else:
            print('All SKUs are R4, R9, or discontinued')
        print (k)
        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

elif data_type == 'sku':
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
        grainger_df = gcom.grainger_q(grainger_attr_query, 'item.MATERIAL_NO', sku_str)
        if grainger_df.empty == False:
            search_level = 'SKU'
            df_stats = get_stats(grainger_df)
            df_fill = get_fill_rate(grainger_df)
            fd.attr_data_out(settings.directory_name, grainger_df, df_stats, df_fill, search_level)
        else:
            print('All SKUs are R4, R9, or discontinued')
        print(search_data)
        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
        
elif data_type == 'name':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass
            else:
                k = "'" + str(k) + "'"
        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"
        df = gcom.grainger_q(grainger_value_query, 'attr.DESCRIPTOR_NAME', k)
        if grainger_df.empty == False:
            fd.data_out(settings.directory_name, grainger_df, search_level, 'ATTRIBUTES')
        else:
            print('No results returned')
        print(k)
        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

elif data_type == 'value':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass
            else:
                k = "'" + str(k) + "'"
        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"
        df = gcom.grainger_q(grainger_value_query, 'item_attr.ITEM_DESC_VALUE', k)
        if df.empty == False:
            fd.data_out(settings.directory_name, df, search_level, 'ATTRIBUTES')
        else:
            print('No results returned')
        print(k)
        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
