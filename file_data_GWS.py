# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:58:51 2019

@author: xcxg109
"""
from pathlib import Path
import pandas as pd
import settings_NUMERIC
import pandas.io.formats.excel
import os
import string


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger Blue (node) \n2. GWS\n3. SKU ")
            if data_type in ['1', 'node', 'Node', 'NODE', 'blue', 'Blue', 'BLUE', 'b', 'B']:
                data_type = 'grainger_query'
                break
            elif data_type in ['2', 'gws', 'Gws', 'GWS', 'g', 'G']:
                data_type = 'gws_query'
                break
            elif data_type in ['3', 'sku', 'Sku', 'SKU', 's', 'S']:
                data_type = 'sku'
                break
        except ValueError:
            print('Invalid search type')
        
    return data_type


def blue_search_level():
    """If data type is node (BLUE data), ask for segment/family/category level for pulling the query. This output feeds directly into the query"""
    while True:
        try:
            search_level = input("Search by: \n1. Segement (L1)\n2. Family (L2)\n3. Category (L3) ")
            if search_level in ['1', 'Segment', 'segment', 'SEGMENT', 'l1', 'L1']:
                search_level = 'cat.SEGMENT_ID'
                break
            elif search_level in ['2', 'Family', 'family', 'FAMILY', 'l2', 'L2']:
                search_level = 'cat.FAMILY_ID'
                break
            elif search_level in ['3', 'Category', 'category', 'CATEGORY', 'l3', 'L3']:
                search_level = 'cat.CATEGORY_ID'
                break
        except ValueError:
            print('Invalid search type')
        
    return search_level


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
#    type_list = ['Node', 'SKU']
    
    if data_type == 'grainger_query':
        search_data = input('Input Blue node ID or hit ENTER to read from file: ')
    elif data_type == 'gws_query':
        search_data = input ('Input GWS terminal node ID or ENTER to read from file: ')
    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')
        
    if search_data != "":
        search_data = [search_data]
        return search_data
    else:
        file_data = settings.get_file_data()

        if data_type == 'grainger_query' or data_type == 'gws_query':
            search_data = [int(row[0]) for row in file_data[1:]]
            return search_data
        elif data_type == 'sku':
            search_data = [row[0] for row in file_data[1:]]
            return search_data        

def modify_name(df, replace_char, replace_with):
    return df.str.replace(replace_char, replace_with)


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def outfile_name (directory_name, quer, df, search_level, gws='no'):
#generate the file name used by the various output functions
    if quer == 'CHECK':
        if gws == 'yes':
            outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,3], '_DATA CHECK')
        else:
            outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,3], '_DATA CHECK') 
                
    else:
        outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,5], df.iloc[0,6], quer)
        
    return outfile


#general output to xlsx file, used for the basic query
def data_out(directory_name, df, quer, search_level):
    """basic output for any GWS query""" 
    os.chdir(directory_name) #set output file path
    
    if df.empty == False:
      #  grainger_df['CATEGORY_NAME'] = modify_name(grainger_df['CATEGORY_NAME'], '/', '_') #clean up node names to include them in file names
        outfile = outfile_name (directory_name, quer, df, search_level)
        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
        df.to_excel (writer, sheet_name="DATA", startrow=0, startcol=0, index=False)
        worksheet = writer.sheets['DATA']
        col_widths = get_col_widths(df)
        col_widths = col_widths[1:]
        
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet.set_column(i, i, width) 
        writer.save()
    else:
        print('EMPTY DATAFRAME')


def data_check_out(directory_name, grainger_df, stats_df, quer, search_level, ws):
    """merge Granger and Gamut data and output as Excel file"""
    
    grainger_df['Category_Name'] = modify_name(grainger_df['Category_Name'], '/', '_') #clean up node names to include them in file names   
 
    columnsTitles = ['Grainger_SKU', 'GWS_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                     'Category_ID', 'Category_Name', 'GWS_Node_ID', 'GWS_Node_Name', 'GWS_PIM_Path']        
    grainger_df = grainger_df.reindex(columns=columnsTitles)
    
    columnsTitles = ['Category_ID', 'Category_Name', 'GWS_Node_ID', 'GWS_Node_Name', '#_Grainger_Attributes', \
                   '#_GWS_Attributes', '#_Grainger_Products', '#_GWS_Products', 'Differing_Attributes']        
    stats_df = stats_df.reindex(columns=columnsTitles)

    outfile = outfile_name (directory_name, quer, grainger_df, search_level, ws)
    
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
#    writer = pd.ExcelWriter('F:\CGabriel\Grainger_Shorties\OUTPUT\moist.xlsx', engine='xlsxwriter')
      
    stats_df.to_excel (writer, sheet_name="Stats", startrow=0, startcol=0, index=False)
    grainger_df.to_excel (writer, sheet_name="All_SKUs", startrow=0, startcol=0, index=False)
   
    worksheet1 = writer.sheets['Stats']
    worksheet2 = writer.sheets['All_SKUs']
    
    col_widths = get_col_widths(stats_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width) 

    col_widths = get_col_widths(grainger_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width) 
  
    writer.save()
