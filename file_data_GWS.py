    # -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:58:51 2019

@author: xcxg109
"""
from pathlib import Path
import pandas as pd
import settings_NUMERIC as settings
import pandas.io.formats.excel
import os
import string


def WS_search_type():
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


def values_search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Blue (node)\n2. Yellow\n3. SKU\n4. Other ")
            if data_type in ['1', 'node', 'Node', 'NODE', 'blue', 'Blue', 'BLUE', 'b', 'B']:
                data_type = 'grainger_query'
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
    elif data_type == 'yellow':
        search_data = input('Input Yellow node ID or hit ENTER to read from file: ')
    elif data_type == 'gws_query':
        search_data = input ('Input GWS terminal node ID or ENTER to read from file: ')
    elif data_type == 'sku':
        search_data = input ('Input SKU or hit ENTER to read from file: ')
    elif data_type == 'value':
        search_data = input ('Input attribute value to search for: ')
    elif data_type == 'name':
        search_data = input ('Input attribute name to search for: ')
    elif data_type == 'supplier':
        search_data = input ('Input Supplier ID to search for: ')
    elif data_type == 'uom_group':
        search_data = input ('Input UOM Group ID to search for: ')
    elif data_type == 'uom_val':
        search_data = input ('Input UOM Value to search for: ')
        
    if search_data != "":
        search_data = search_data.strip()
        search_data = [search_data]
        return search_data

    else:
        file_data = settings.get_file_data()

        if data_type == 'sku':
            search_data = [row[0] for row in file_data[1:]]
            return search_data
        elif data_type == 'yelow':
            search_data = [str(row[0]) for row in file_data[1:]]
            return search_data            
        else:
            search_data = [int(row[0]) for row in file_data[1:]]
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
    if search_level == 'SKU':
        outfile = Path(directory_name)/"SKU REPORT.xlsx"

    else:   
        if gws == 'yes':
            if search_level == 'cat.SEGMENT_ID':    #set directory path and name output file
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,2], df.iloc[0,3], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,6], df.iloc[0,7], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,0], df.iloc[0,1], quer)

        elif quer == 'CHECK':
            if search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,2], df.iloc[0,3], '_DATA CHECK')
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,6], df.iloc[0,7], '_DATA CHECK') 

        elif quer == 'ATTRIBUTES' or quer == 'ETL':
            if search_level == 'cat.CATEGORY_ID':
                outfile = Path(directory_name)/"{} {}.xlsx".format(df.iloc[0,5], quer)                
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,3], df.iloc[0,4], quer)                
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,1], df.iloc[0,2], quer)

        elif quer == 'DESC':
            if search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,1], df.iloc[0,2], quer)
            else:
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,5], df.iloc[0,6], quer)
            
        elif quer == 'HIER':
            if search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,2], df.iloc[0,3], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,4], df.iloc[0,5], quer)                
            if search_level == 'cat.CATEGORY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,6], df.iloc[0,7], quer)                
            
        else:
            if search_level == 'cat.CATEGORY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,4], df.iloc[0,5], quer)                
            elif search_level == 'cat.SEGMENT_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,0], df.iloc[0,1], quer)
            elif search_level == 'cat.FAMILY_ID':
                outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,3], df.iloc[0,4], quer)
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
                     'Category_ID', 'Category_Name', 'GWS_Node_ID', 'GWS_Node_Name', 'GWS_PIM_Path', \
                     'PM_CODE', 'SALES_STATUS', 'RELATIONSHIP_MANAGER_CODE']        
    grainger_df = grainger_df.reindex(columns=columnsTitles)
    
    columnsTitles = ['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', \
                    'GWS_Node_ID', 'GWS_Node_Name', '#_Grainger_Attributes', '#_GWS_Attributes', \
                    '#_Grainger_Products', '#_GWS_Products', 'Grainger_Attributes', 'GWS_Attributes', 'Differing_Attributes']        
    stats_df = stats_df.reindex(columns=columnsTitles)
    stats_df = stats_df.sort_values(['Category_Name'], ascending=[True])

    outfile = outfile_name (directory_name, quer, grainger_df, search_level, ws)
    
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book
      
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
  
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    worksheet1.set_column('M:M', 60, layout)
    worksheet1.set_column('N:N', 60, layout)
    worksheet1.set_column('O:O', 60, layout)

    writer.save()


#output for attribute values for Grainger
def attr_data_out(directory_name, df, df_stats, df_fill, search_level):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    df.drop(columns=['Count'], inplace=True)
    
    quer = 'ATTRIBUTE'
    
    columnTitles = ['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', \
                    'PM_Code', 'Sales_Status', 'RELATIONSHIP_MANAGER_CODE', 'PIM_Path', 'WS_Category_ID', \
                    'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'Grainger_SKU', 'WS_SKU', 'Grainger_Attr_ID', \
                    'Attribute_Value_ID', 'Data_Type', 'Grainger_Attribute_Name', 'WS_Attribute_Name',\
                    'Original_Value', 'Original_Unit', 'Normalized_Value', 'Normalized_Unit', \
                    'Grainger_Attribute_Value', 'WS_Value', 'STEP-WS_Match?', 'Potential_Replaced_Values', \
                    'Revised_Value']

    df = df.reindex(columns=columnTitles)

    outfile = outfile_name (directory_name, quer, df, search_level)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    
    # Write each dataframe to a different worksheet.
    df_stats.to_excel(writer, sheet_name='Stats', startrow=1, startcol=0)
    df_fill.to_excel(writer, sheet_name='Stats', startrow =5, startcol=5, index=False)
    df.to_excel(writer, sheet_name='Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet1 = writer.sheets['Stats']
    worksheet2 = writer.sheets['Data']
    
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')

    #setup display for Stats sheet
    worksheet1.set_column('A:A', 40, layout)
    worksheet1.set_column('B:B', 60, layout)
    worksheet1.set_column('F:F', 40, layout)
    worksheet1.set_column('G:G', 15, num_layout)
    worksheet1.set_column('H:H', 20, layout)
    
    col_widths = get_col_widths(df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width) 

    writer.save()


#output for attribute values for Grainger
def GWS_upload_data_out(directory_name, df, search_level):
       
    quer = 'ETL'
        
    columnTitles = ['STEP Blue Path','Segment ID','Segment Name','Family ID','Family Name','STEP Category ID', \
                'Category Name','STEP Attribute ID','STEP Attribute Name','WS Node ID', \
                'WS Node Name','WS Attribute ID','WS Attribute Name','Attribute Name','Definition',\
                'Data Type','Multivalued?','Group','Group Type','Group Role','Group Parameter',\
                'Restricted Attribute Value Domain','Unit of Measure Domain (Group ID)','Sample Values',\
                'Numeric Display Type','Notes','Recommended Data Type','%_Numeric','Potential UOMs', \
                'String Values (for Number Data Type)','Recommended Unit of Measure ID', \
                'Definition Source','Matching','Grainger ALL Values','Comma Separated Values']
    
    df = df.reindex(columns=columnTitles)

    # Write each dataframe to a different worksheet.
    df = df.sort_values(['Segment Name', 'Category Name', 'Attribute Name'], ascending=[True, True, True])

    outfile = outfile_name (directory_name, quer, df, search_level)
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
   
    # Write each dataframe to a different worksheet.
    df.to_excel(writer, sheet_name='Upload Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Upload Data']
  
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

    worksheet.set_column('O:O', 70, layout)
    worksheet.set_column('V:V', 70, layout)
    worksheet.set_column('X:X', 50, layout)
    worksheet.set_column('Z:Z', 40, layout)

    highlight_cols = ['Recommended Data Type','%_Numeric','Potential UOMs','String Values (for Number Data Type)',\
                        'Recommended Unit of Measure ID','Definition Source', \
                        'Matching','Grainger ALL Values','Comma Separated Values', 'Recommended Unit of Measure ID']
    
    # add blue highlight to 'helper' columns that will be deleted for import
    color_format = workbook.add_format({'bg_color': 'add8e6'})
    
    for col in highlight_cols:
        excel_col = int(df.columns.get_loc(col))
        len_df = int(len(df.index) + 1)

        # conditional formating to cover ALL cells, blank and populated
        worksheet.conditional_format(1, excel_col, len_df, excel_col, {'type': 'blanks', 'format': color_format})
        worksheet.conditional_format(1, excel_col, len_df, excel_col, {'type': 'no_blanks', 'format': color_format})

    writer.save()
    
    
def shorties_data_out(directory_name, grainger_df, gws_df, search_level):
    """merge Granger and Gamut data and output as Excel file"""
    
    gws = 'no'
    quer = 'DESC'
    grainger_df['Category_Name'] = modify_name(grainger_df['Category_Name'], '/', '_') #clean up node names to include them in file names   
 
    #if gamut data is present for these skus, merge with grainger data
    if gws_df.empty == False:
        gws = 'yes'
        grainger_df = grainger_df.merge(gws_df, how="left", on=["WS_SKU"])
        
        columnTitles = ['WS_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                        'Category_Name', 'BRAND_NAME', 'PM_CODE', 'Item_Description', 'SEO_Description', \
                        'WS_Product_Description', 'WS_Merch_Node', 'STEP_Yellow']
    
        grainger_df = grainger_df.reindex(columns=columnTitles)
        outfile = outfile_name (directory_name, quer, grainger_df, search_level, gws)

    else:
        outfile = outfile_name (directory_name, quer, grainger_df, search_level)

        columnTitles = ['WS_SKU', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                        'Category_Name', 'BRAND_NAME', 'PM_CODE', 'Item_Description', 'SEO_Description', 'STEP_Yellow']

        grainger_df = grainger_df.reindex(columns=columnTitles)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
      
    grainger_df.to_excel (writer, sheet_name="Shorties", startrow=0, startcol=0, index=False)
   
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Shorties']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    col_widths = get_col_widths(grainger_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
    
    if gws == 'yes':
        worksheet.set_column('K:K', 70, layout)
        worksheet.set_column('L:L', 70, layout)
    else:
        worksheet.set_column('K:K', 70, layout)
        
    writer.save()    
    
    
def hier_data_out(directory_name, df, quer, stat, search_level):

    columnTitles = ['Grainger_SKU', 'WS_SKU', 'PIM_ID', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                    'Category_ID', 'Category_Name', 'Gcom_Yellow', 'Gcom_Web_Parent', 'GWS_PIM_Path', \
                    'GWS_Category_ID', 'GWS_Category_Name', 'GWS_Node_ID', 'GWS_Node_Name', 'PM_CODE', \
                    'SALES_STATUS', 'RELATIONSHIP_MANAGER_CODE']    
    df = df.reindex(columns=columnTitles)

    outfile = outfile_name(directory_name, quer, df, search_level, stat)
  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
      
    df.to_excel (writer, sheet_name="Hierarchy", startrow=0, startcol=0, index=False)
   
    worksheet = writer.sheets['Hierarchy']
    
    col_widths = get_col_widths(df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
        
    writer.save()