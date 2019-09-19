# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:58:51 2019

@author: xcxg109
"""
from pathlib import Path
import pandas as pd
import settings
import pandas.io.formats.excel


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



def modify_name(df, replace_char, replace_with):
    return df.str.replace(replace_char, replace_with)


def col_order(df, order):
    """arrange data for output"""
    cols = df.columns.tolist()
    cols = [cols[i] for i in order]
    df = df[cols]
    return df


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
#    type_list = ['Node', 'SKU']
    
    if data_type == 'node':
        search_data = input('Input Blue node ID or hit ENTER to read from file: ')
    elif data_type == 'yellow':
        search_data = input('Input Yellow node ID or hit ENTER to read from file: ')
    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')
        
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


def outfile_name (directory_name, quer, df, search_level, gamut='no'):
#generate the file name used by the various output functions
    if search_level == 'SKU':
        outfile = Path(directory_name)/"SKU REPORT.xlsx"
    else:   
        if quer == 'GRAINGER-GAMUT':
            if search_level == 'cat.SEGMENT_ID':    #set directory path and name output file
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,1], df.iloc[0,2], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,3], df.iloc[0,4], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,5], df.iloc[0,6], quer)
        else:
            if search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,1], df.iloc[0,2], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,3], df.iloc[0,4], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,5], df.iloc[0,6], quer)
    return outfile
    

def attribute_match_data_out(directory_name, df, search_level):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    df['Category_Name'] = modify_name(df['Category_Name'], '/', '_') #clean up node names to include them in file names       

    quer = 'GRAINGER-GAMUT'
    
    order = [10, 0, 1, 2, 3, 4, 5, 14, 15, 16, 13, 17, 8, 9, 20, 24, 23, 11, 21, 6, 7, 25, 26, 27, 28, 29, 18, 19] 
    df = col_order(df, order)
    outfile = outfile_name (directory_name, quer, df, search_level)
    
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    
    pd.io.formats.excel.header_style = None
    
    # Write each dataframe to a different worksheet.
    df.to_excel(writer, sheet_name='Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet1 = writer.sheets['Data']
    
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    header_fmt = workbook.add_format()
    header_fmt.set_text_wrap('text_wrap')
    header_fmt.set_bold()
    #header_fmt.set_bg_color('#FF0000')

    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')
                              
        
    #setup display for Stats sheet
    #set header different
    worksheet1.set_row(0, None, header_fmt)
    
    worksheet1.set_column('A:A', 30, layout)
    worksheet1.set_column('B:B', 12, layout)
    worksheet1.set_column('C:C', 30, layout)
    worksheet1.set_column('D:D', 12, layout)
    worksheet1.set_column('E:E', 30, layout)
    worksheet1.set_column('F:F', 12, layout)
    worksheet1.set_column('G:G', 30, layout)
    worksheet1.set_column('H:H', 30, layout)
    worksheet1.set_column('I:I', 12, layout)
    worksheet1.set_column('J:J', 30, layout)
    worksheet1.set_column('K:K', 12, layout)
    worksheet1.set_column('L:L', 30, layout)
    worksheet1.set_column('M:M', 40, layout)
    worksheet1.set_column('N:N', 40, layout)
    worksheet1.set_column('O:O', 40, layout)
    worksheet1.set_column('P:P', 30, layout)
    worksheet1.set_column('Q:Q', 20, layout)
    worksheet1.set_column('R:R', 30, layout)
    worksheet1.set_column('S:S', 30, layout)
    worksheet1.set_column('T:T', 12, layout)
    worksheet1.set_column('U:U', 30, layout)
    worksheet1.set_column('V:V', 30, layout)
    worksheet1.set_column('W:W', 30, layout)
    worksheet1.set_column('X:X', 30, layout)
    worksheet1.set_column('Y:Y', 15, layout)
    worksheet1.set_column('Z:Z', 30, layout)
    worksheet1.set_column('AA:AA', 12, layout)
    worksheet1.set_column('AB:AB', 30, layout)
        
    writer.save()