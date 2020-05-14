# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:58:51 2019

@author: xcxg109
"""
from pathlib import Path
import pandas as pd
import settings
import pandas.io.formats.excel
import os


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger Blue (node) \n2. Gamut\n3. SKU ")
            if data_type in ['1', 'node', 'Node', 'NODE', 'blue', 'Blue', 'BLUE', 'b', 'B']:
                data_type = 'grainger_query'
                break
            elif data_type in ['2', 'gamut', 'Gamut', 'GAMUT', 'g', 'G']:
                data_type = 'gamut_query'
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

    if search_level == 'cat.SEGMENT_ID':
        while True:
            try:
                data_process = input('Process type: \n1. All \n2. Individual Nodes ')
                if data_process in ['1', 'all', 'All', 'ALL']:
                    data_process = 'one'
                    break
                elif data_process in ['2', 'node', 'Node', 'NODE']:
                    data_process = "two"
                    break
            except ValueError:
                print('Invalid process type')
    else:
        data_process = "two"
        
    return search_level, data_process


def modify_name(df, replace_char, replace_with):
    return df.str.replace(replace_char, replace_with)


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
#    type_list = ['Node', 'SKU']
    
    if data_type == 'grainger_query':
        search_data = input('Input Blue node ID or hit ENTER to read from file: ')
    elif data_type == 'gamut_query':
        search_data = input ('Input Gamut terminal node ID or ENTER to read from file: ')
    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')
        
    if search_data != "":
        search_data = [search_data]
        return search_data
    else:
        file_data = settings.get_file_data()

        if data_type == 'grainger_query' or data_type == 'gamut_query':
            search_data = [int(row[0]) for row in file_data[1:]]
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
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,2], df.iloc[0,3], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,4], df.iloc[0,5], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,6], df.iloc[0,7], quer)
        else:
            if search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,1], df.iloc[0,2], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,3], df.iloc[0,4], quer)
            elif quer == 'HIER':
                if gamut == 'yes':
                    outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,7], quer)
                else:
                    outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,6], quer) 
            elif quer == 'ATTRIBUTES':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,4], df.iloc[0,5], quer)
            elif quer == 'ATTR':
                outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,3], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,5], df.iloc[0,6], quer)
    return outfile

    
def attribute_match_data_out(directory_name, df, search_level):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    df['Category_Name'] = modify_name(df['Category_Name'], '/', '_') #clean up node names to include them in file names       

    quer = 'GRAINGER-GAMUT'
    
    #output raw files before organizing/editing columns for human file

    columnsTitles = ['Gamut/Grainger SKU Counts', 'Grainger Blue Path', 'Segment_ID', 'Segment_Name', \
                     'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', 'Gamut_PIM_Path', \
                     'Gamut_Category_ID', 'Gamut_Category_Name', 'Gamut_Node_ID', 'Gamut_Node_Name', \
                     'Grainger-Gamut Terminal Node Mapping', 'Grainger_Attr_ID', 'Grainger_Attribute_Name',\
                     'Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Matching', 'ENDECA_RANKING', 'Grainger_Fill_Rate_%', 'Grainger_Attribute_Definition', \
                     'Grainger_Category_Specific_Definition', 'Gamut_Attribute_Definition',\
                     'Grainger Attribute Sample Values', 'Gamut Attribute Sample Values']
    
    df = df.reindex(columns=columnsTitles)
    outfile = outfile_name (directory_name, quer, df, search_level)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    
    pd.io.formats.excel.header_style = None
    
    # Write each dataframe to a different worksheet.
    df = df.sort_values(['Segment_Name', 'Category_Name', 'Gamut_Node_Name', 'Grainger_Attribute_Name', \
                         'Gamut_Attribute_Name'], ascending=[True, True, True, True, True])
    df.to_excel(writer, sheet_name='Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Data']
    
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    header_fmt = workbook.add_format()
    header_fmt.set_text_wrap('text_wrap')
    header_fmt.set_bold()

    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')
    
    col_widths = get_col_widths(df)
    col_widths = col_widths[1:]

    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
    writer.save()
                               
        
def numbers_out(directory_name, df, search_level):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    df['Category_Name'] = modify_name(df['Category_Name'], '/', '_') #clean up node names to include them in file names       

    quer = 'GRAINGER-GAMUT'
    
    #output raw files before organizing/editing columns for human file

    columnsTitles = ['Gamut/Grainger SKU Counts', 'Grainger Blue Path', 'Segment_ID', 'Segment_Name', \
                     'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', 'Gamut_PIM_Path', \
                     'Gamut_Category_ID', 'Gamut_Category_Name', 'Gamut_Node_ID', 'Gamut_Node_Name', \
                     'Grainger-Gamut Terminal Node Mapping', 'Grainger_Attr_ID', 'Grainger_Attribute_Name',\
                     'Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Matching', 'ENDECA_RANKING', 'Grainger_Fill_Rate_%', 'Grainger_Attribute_Definition', \
                     'Grainger_Category_Specific_Definition', 'Gamut_Attribute_Definition',\
                     'Grainger Attribute Sample Values']
#                     'alt_grainger_name', 'alt_gamut_name'
    
    df = df.reindex(columns=columnsTitles)
    outfile = outfile_name (directory_name, quer, df, search_level)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    
    pd.io.formats.excel.header_style = None
    
    # Write each dataframe to a different worksheet.
    df = df.sort_values(['Segment_Name', 'Category_Name', 'Gamut_Node_Name', 'Grainger_Attribute_Name', \
                         'Gamut_Attribute_Name'], ascending=[True, True, True, True, True])
    df.to_excel(writer, sheet_name='Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Data']
    
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    header_fmt = workbook.add_format()
    header_fmt.set_text_wrap('text_wrap')
    header_fmt.set_bold()

    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')
    
    col_widths = get_col_widths(df)
    col_widths = col_widths[1:]

    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
    writer.save()


#general output to xlsx file, used for the basic query
def data_out(directory_name, df, quer, search_level):
    """basic output for any Gamut query""" 
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
        
def hier_data_out(directory_name, grainger_df, gamut_df, quer, search_level):
    """merge Granger and Gamut data and output as Excel file"""
    
    grainger_df['Category_Name'] = modify_name(grainger_df['Category_Name'], '/', '_') #clean up node names to include them in file names   
 
    #if gamut data is present for these skus, merge with grainger data
    if gamut_df.empty == False:
        gamut = 'yes'
        grainger_df = grainger_df.merge(gamut_df, how="left", on=["Grainger_SKU"])
        columnsTitles = ['Grainger_SKU', 'Gamut_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                         'Category_ID', 'Category_Name', 'PM_CODE', 'SALES_STATUS', 'PIM Node ID', 'tax_path']
        grainger_df = grainger_df.reindex(columns=columnsTitles)
        outfile = outfile_name (directory_name, quer, grainger_df, search_level, gamut)
    else:
        outfile = outfile_name (directory_name, quer, grainger_df, search_level)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
      
    grainger_df.to_excel (writer, sheet_name="Shorties", startrow=0, startcol=0, index=False)
   
    worksheet1 = writer.sheets['Shorties']
    
    col_widths = get_col_widths(grainger_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
    writer.save()
  
    writer.save()


#output for attribute values for Grainger
def GWS_upload_data_out(directory_name, df_upload, df_summary, search_level):

    df['Category_Name'] = modify_name(df['Category_Name'], '/', '_') #clean up node names to include them in file names   
    pd.io.formats.excel.header_style = None
    
    quer = 'ATTRIBUTES'
    
    columnsTitles = ['Category_ID', 'Category_Name', 'Grainger_Attr_ID', 'Grainger_Attribute_Name', \
                     'Fill_Rate_%', '%_Numeric', 'Candidate', 'Grainger ALL Values']
    df_summary = df_summary.reindex(columns=columnsTitles)
    
    columnTitles = ['STEP Blue Path' , 'Segment ID' , 'Segment Name' , 'Family ID' , 'Family Name' , \
                'Category ID' , 'Category Name' , 'Attribute_ID' , 'Attribute Name' , 'Definition' , \
                'Data Type' , 'Multivalued?' , 'Group' , 'Group Type' , 'Group Role' , \'Group Parameter' , \
                'Restricted Attribute Value Domain' , 'Unit of Measure Domain' , 'Sample Values' , \
                'Numeric display type' , 'Matching' , 'Gamut Attribute Sample Values']
    df_upload = df_upload.reindex(columns=columnsTitles)

    # define Candidate sort order
    sorter = ['Y', 'potential', 'N']
    # dictionary that defines the order for sorting
    sorterIndex = dict(zip(sorter,range(len(sorter))))
    # generate rank column to sort df
    df_summary['Can_Rank'] = df_summary['Candidate'].map(sorterIndex)

    df_summary.sort_values(['Category_Name', 'Can_Rank', 'Grainger_Attribute_Name'], \
                        ascending=[True, True, True], inplace = True)
    df_summary.drop('Can_Rank', 1, inplace=True)
    outfile = outfile_name (directory_name, quer, df, search_level)
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
   
    # Write each dataframe to a different worksheet.
    df_summary.to_excel(writer, sheet_name='Summary', startrow =1, startcol=0, index=False)
    df_upload.to_excel(writer, sheet_name='Upload Data', index=False)
#    df_stats.to_excel(writer, sheet_name='Data', startrow=1, startcol=0)
#    df.to_excel(writer, sheet_name='Raw', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet1 = writer.sheets['Summary']
    worksheet2 = writer.sheets['Upload Data']
 #   worksheet3 = writer.sheets['Raw']
  
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    header_fmt = workbook.add_format()
    header_fmt.set_text_wrap('text_wrap')
    header_fmt.set_bold()

    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')

    col_widths = get_col_widths(df_summary)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width) 

    col_widths = get_col_widths(df_upload)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width) 

    #setup display for Data sheet
#    worksheet2.set_column('A:A', 30, layout)
#    worksheet2.set_column('B:B', 60, layout)
    
    #steup display for Raw sheet
#    worksheet3.set_column('F:F', 25, layout)
#    worksheet3.set_column('G:G', 30, layout)
#    worksheet3.set_column('H:H', 60, layout)
    
    writer.save()
