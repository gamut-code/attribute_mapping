# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
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


def grainger_values(df):
    """aggregate all valeus for each attribute and calculate fill rates for browseable SKUs"""

    all_vals = pd.DataFrame()
    browsable_skus = pd.DataFrame()

    df['Count'] =1
    atts = df['Grainger_Attribute_Name'].unique()

    vals = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
    
    for attribute in atts:
        temp_att = vals.loc[vals['Grainger_Attribute_Name']== attribute]
               
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df = temp_df.drop_duplicates(subset='Grainger_Attribute_Value')
        temp_df['Grainger ALL Values'] = '; '.join(item for item in temp_df['Grainger_Attribute_Value'] if item)
        all_vals= pd.concat([all_vals, temp_df], axis=0)

    all_vals = all_vals.drop_duplicates(subset='Grainger_Attr_ID')
    all_vals = all_vals[['Grainger_Attr_ID', 'Grainger ALL Values']]

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
        df = df[['Category_ID', 'Category_Name', 'Grainger_Attribute_Name', 'Grainger_Attr_ID', 'ENDECA_Ranking']]
        df = df.drop_duplicates(subset='Grainger_Attr_ID')
        fill_rate = fill_rate.merge(df, how= "inner", on=['Grainger_Attribute_Name'])
        
    else:
        df['Grainger_Fill_Rate_%'] = 'no browsable SKUs'
        fill_rate = df[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate['Grainger_Fill_Rate_%'] = 'no browsable SKUs'

    all_vals = all_vals.merge(fill_rate, how='inner', on=['Grainger_Attr_ID'])  
    
    return all_vals
    

def split(df):
    """ split values into numerators + UOMs and create separate columns for each"""
    all_vals = pd.DataFrame()

    atts = df['Grainger_Attribute_Name'].unique()

    for attribute in atts:
        #put all attribute values into a single string for TF-IDF processing later
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df['Numeric'] = ""
        temp_df['String'] = ""
            
        for row in temp_df.itertuples():
            value = row.Grainger_Attribute_Value
            
            r = re.compile('^\d*[\.\/]?\d*')

            temp_df.at[row.Index, 'Numeric'], temp_df.at[row.Index, 'String'] = re.split(r, value)
            num = r.search(value)           
            temp_df.at[row.Index, 'Numeric'] = num.group()

        all_vals = pd.concat([all_vals, temp_df], axis=0)

    return all_vals


def analyze(df, sum):
    """use the split fields in grainger_df to analyze suitability for number conversion and included in summary df"""

    atts = df['Grainger_Attribute_Name'].unique()

    sum['%_Numeric'] = ''
    sum['Candidate'] = ''

#    vals = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
#    vals = vals.reset_index()
    
    for attribute in atts:
        temp_att = df.loc[df['Grainger_Attribute_Name']== attribute]

        row_count = len(temp_att.index)
        print ('attribute {}: row count = {}'.format(attribute, row_count))
        # Get a bool series representing positive 'Num' rows
        seriesObj = temp_att.apply(lambda x: True if x['Numeric'] != "" else False , axis=1) 
        # Count number of True in series
        num_count = len(seriesObj[seriesObj == True].index)
        percent = num_count/row_count*100

        # build a list of items that are exluded as potential UOM values
        exclusions = ['NEF', 'NPT', 'NPS', 'UNEF', 'Steel']
        
        temp_att['exclude'] = temp_att['String'].apply(lambda x: ','.join([i for i in exclusions if i in x]))
        excludeObj = temp_att.apply(lambda x: True if x['exclude'] != "" else False , axis=1)
        exclude_count = len(excludeObj[excludeObj == True].index)
        exclude_percent = exclude_count/row_count*100

        print('exclude count = ', exclude_count)
        print('row count     = ', row_count)
        print('exclude percent ', exclude_percent)
        sum.loc[sum['Grainger_Attribute_Name'] == attribute, '%_Numeric'] = float(percent)
        
   !!     if 'Thread Size' in attribute or 'Thread Depth' in attribute:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'
        elif exclude_percent > 80:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'        
        elif percent < 80:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'N'
        elif percent >= 80 and percent < 100:           
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'potential'
        elif percent == 100:
            sum.loc[sum['Grainger_Attribute_Name'] == attribute, 'Candidate'] = 'Y'
        
    sum['%_Numeric'] = sum['%_Numeric'].map('{:,.2f}'.format)

    return sum

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
            df_summary = grainger_values(grainger_df)
            grainger_df = split(grainger_df)
            df_summary = analyze(grainger_df, df_summary)
            fd.attr_data_out(settings.directory_name, grainger_df, df_stats, df_summary, search_level)
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
            df_summary = grainger_values(grainger_df)
            fd.attr_data_out(settings.directory_name, grainger_df, df_stats, df_summary, search_level)
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
