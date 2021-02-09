# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 15:56:20 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
import os
from tkinter.filedialog import askdirectory 
import glob
import time

gws = GWSQuery()
pd.options.mode.chained_assignment = None

ws_category="""
    SELECT cat.name

    FROM    taxonomy_category as cat
    
    WHERE {} IN ({})
"""

def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def create_buildsheet(metadata, att_data, high_touch, category_ID, category_name):
    # transpose values for each chunk of the dataframe
    # iterate through the temp_df and if we already have an entry for the attribute, add row data to that entry
    # otherwise, create a new entry with attribute name and attribute ID
    temp_sheet = pd.DataFrame()

    series_row = metadata[metadata['Header'].str.contains("Series")]
    if series_row.empty == False:
        metadata.loc[metadata['Header'].str.contains('Series'),'Header'] = 'Series'

    # gather all the SKU-specific data from the rows of the meta df
    sku = metadata.loc[metadata['Header'] == 'Grainger Part Number', 'Value'].item()
    status = metadata.loc[metadata['Header'] == 'Status', 'Value'].item()
    noun = metadata.loc[metadata['Header'] == 'Primary Noun', 'Value'].item()
    supplier = metadata.loc[metadata['Header'] == 'Supplier', 'Value'].item()
    supplier_part = metadata.loc[metadata['Header'] == 'Supplier Part Number', 'Value'].item()
    manufacturer = metadata.loc[metadata['Header'] == 'Manufacturer', 'Value'].item()
    series = metadata.loc[metadata['Header'] == 'Series', 'Value'].item()
    mfr_part = metadata.loc[metadata['Header'] == 'Mfr Part Number', 'Value'].item()

    for row in att_data.itertuples():
        att_ID = att_data.at[row.Index, 'Attribute_ID']
        att_ID = str(att_ID)
        att_ID = att_ID.strip()        
        
        att_name = att_data.at[row.Index, 'Attribute_Name']
        att_name = str(att_name)
        att_name = att_name.strip()        

        head = att_data.at[row.Index, 'Header']
        head = str(head)
        head = head.strip()

        val = att_data.at[row.Index, 'Value']
        val = str(val)
        val = val.strip()
        
        if temp_sheet.empty == False:
            hightouch_status = ''
            
            if high_touch.empty == True:
                hightouch_status = 'N'
            else:
                for row in high_touch.itertuples():
                    hightouch_node = high_touch.at[row.Index, 'Taxonomy Node']
                    hightouch_node = str(hightouch_node)
                    hightouch_node = hightouch_node.strip()
                    
                    if hightouch_node in category_name:                    
                        hightouch_att = high_touch.at[row.Index, 'Attribute Name']
                        hightouch_att = str(hightouch_att)
                        hightouch_att = hightouch_att.strip()
                
                        if hightouch_att == att_name:                        
                            hightouch_status = high_touch.at[row.Index, 'High Touch']
                            hightouch_status = str(hightouch_status)
                            hightouch_status = hightouch_status.strip()
                                        
            temp_sheet[head] = val
            temp_sheet['High Touch'] = hightouch_status
            temp_sheet = temp_sheet.combine_first(temp_sheet)

        else:
           temp_sheet = temp_sheet.append({'Category ID': category_ID,
                                       'Category Name': category_name,
                                       'Grainger Part Number': sku,
                                       'Status': status,
                                       'Primary Noun': noun,
                                       'Supplier': supplier,
                                       'Supplier Part Number': supplier_part,
                                       'Manufacturer': manufacturer,
                                       'Series': series,
                                       'Mfr Part Number': mfr_part,
                                       'Attribute_ID': att_ID,
                                       'Attribute Name': att_name,
                                       head: val
                                       }, ignore_index=True)
                    
    return temp_sheet


def data_out(final_df, high_touch, batch=''):
    # get rid of all 'nan' values in df / clean up final_df
    final_df = final_df.replace(np.nan, '', regex=True)
    final_df = final_df.replace('nan', '')
    final_df['High Touch'] = final_df['High Touch'].str.upper()
    final_df = final_df.drop_duplicates()
    
    if high_touch.empty == False:
        # clean up high_touch df
        high_touch = high_touch[['Taxonomy Node', 'Attribute Name', 'Definition', 'Sample Values', 'High Touch']]    
        high_touch = high_touch.drop_duplicates()
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/Audit_Buildsheet_'+str(batch)+'.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    final_df.to_excel (writer, sheet_name="Build Sheet", startrow=0, startcol=0, index=False)
    worksheet1 = writer.sheets['Build Sheet']
        
    col_widths = get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    if high_touch.empty == False:
        high_touch.to_excel (writer, sheet_name="Attribute Reference", startrow=0, startcol=0, index=False)
        worksheet2 = writer.sheets['Attribute Reference']

        col_widths = get_col_widths(high_touch)
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

    writer.save()

        
print('Choose build sheet directory')
start_time = time.time()

init_dir = 'R:/PM/Hsueh/Audit Template Conversion'

path = askdirectory(initialdir=init_dir)

os.chdir(path)
file_list = glob.glob('*.xlsx')
hightouch_df = pd.DataFrame()

print('Processing {} files'.format(len(file_list)))

#create empty Build Sheet template
column_names = ['Category ID', 'Category Name', 'Grainger Part Number', 'Status', 'Primary Noun', 'Supplier', \
                'Supplier Part Number', 'Manufacturer', 'Series', 'Mfr Part Number', 'Attribute_ID', \
                'High Touch', 'Attribute Name', 'Value', 'Unit', 'Source', 'Link', 'Image', 'Decision Log']
buildsheet_df = pd.DataFrame(columns = column_names)

for file in file_list:
    filename = os.fsdecode(file)
    filename = filename.lower()
    # read in the sheet name and parse for the category ID and name

    if 'high touch' in filename or 'high_touch' in filename:
        xls = pd.ExcelFile(file)
        hightouch_df = pd.read_excel(xls, 'Schema')

if hightouch_df.empty == True:
    print('CANNOT FIND HIGH TOUCH FILE')

file_count = 1

for file in file_list:
    file_time = time.time()

    filename = os.fsdecode(file)
    filename = filename.lower()
    # read in the sheet name and parse for the category ID and name

    if 'high touch' in filename or 'high_touch' in filename:
        pass # do nothing -- we've already read this file above
        
    else:
        # read sheet name, which in buildsheet export contains the Category ID        
        xls = pd.ExcelFile(file)
        file_node = str(xls.sheet_names)
        cat_ID = file_node.split('-')
        cat_ID = cat_ID[0]
        cat_ID = cat_ID[2:].strip()
        
        # read in only attribute names and values, skipping buildsheet metadata rows
        main_df = pd.read_excel(filename, 
                           skiprows= (1,2,3,4,6,7),
                           header=None)
    
        cat_name = gws.gws_q(ws_category, 'cat.id', cat_ID)
        cat_name = cat_name['name'].unique()
        cat_name = cat_name[0]

        # flip buildsheet read-in to get it closer to our final format
        main_df = main_df.T 
        main_df[0].fillna(method='ffill', inplace=True)
        main_df[1].fillna(method='ffill', inplace=True)

        main_df.dropna(how='all', axis=1)
    
        # determine number of SKUs = number of times to iterateg
        skus = len(main_df.columns)
        skus = skus - 3     # remove non-SKU columns from count
        
        print('{}. {} : {} skus '.format(file_count, cat_name, skus))

        # set starting column for reading first SKU data
        count = 3
        
        for sku in range(skus):
            sku_time = time.time()
            # create unique df for each SKU
            sku_df = main_df[[0, 1, 2, count]]
            sku_df.columns = ['Attribute_ID', 'Attribute_Name', 'Header', 'Value']
            
            # separate SKU data into separate df and remove from attribute data
            meta = sku_df[sku_df['Attribute_Name'] == 'Attributes:']
            meta = meta[['Header', 'Value']]    # drop rows 0 ('Category ID') and 1 ('Attribute:')

            sku_df = sku_df[sku_df['Attribute_Name'] != 'Attributes:']        
 
            attributes = sku_df['Attribute_Name'].unique().tolist()
            
            for att in attributes:
                att_df = sku_df[sku_df['Attribute_Name'] == att]
                temp_build_df = create_buildsheet(meta, att_df, hightouch_df, cat_ID, cat_name)
            
                buildsheet_df = pd.concat([buildsheet_df, temp_build_df], axis=0, sort=False)
            
            #increment column count so metadata/data for the next sku is pulled
            count += 1
            
    print("file time = {} minutes ---".format(round((time.time() - file_time)/60, 2)))
    file_count += 1
    
if len(buildsheet_df) > 900000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(buildsheet_df)/900000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(buildsheet_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, hightouch_df, count)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(buildsheet_df, hightouch_df)
        
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))