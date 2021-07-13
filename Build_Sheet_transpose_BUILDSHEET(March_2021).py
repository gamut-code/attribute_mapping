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

    # gather all the SKU-specific data from the rows of the meta df
    sku = metadata.loc[metadata['Header'] == 'Grainger Part Number', 'Value'].item()
    status = metadata.loc[metadata['Header'] == 'Status', 'Value'].item()
    noun = metadata.loc[metadata['Header'] == 'Primary Noun', 'Value'].item()
    supplier = metadata.loc[metadata['Header'] == 'Supplier', 'Value'].item()
    supplier_part = metadata.loc[metadata['Header'] == 'Supplier Part Number', 'Value'].item()
    manufacturer = metadata.loc[metadata['Header'] == 'Manufacturer', 'Value'].item()
    series = metadata.loc[metadata['Header'] == 'Series', 'Value'].item()
    mfr_part = metadata.loc[metadata['Header'] == 'Manufacturer Part Number', 'Value'].item()
                                                                                                                                                                                                                                                                
    hightouch_col = [col for col in high_touch.columns if 'High Touch' in col]
    if not hightouch_col:
        hightouch_col = [col for col in high_touch.columns if 'High-Touch' in col]    

    tax_col = [col for col in high_touch.columns if 'Taxonomy' in col]
    if not tax_col:
        tax_col = [col for col in high_touch.columns if 'Node Name' in col]
    
    if len(high_touch[tax_col]) == 0:
        tax_col = [col for col in high_touch.columns if 'Current Node' in col]
    
    att_col = [col for col in high_touch.columns if 'Attribute Name' in col]
    if not att_col:
        att_col = [col for col in high_touch.columns if 'Attribute' in col]

    att_ID = att_data['Attribute_ID'].unique().tolist()
    att_ID = str(att_ID[0]).strip()

    att_name = att_data['Attribute_Name'].unique().tolist()
    att_name = str(att_name[0]).strip()
    
    data_type = att_data['Data Type'].unique().tolist()
    data_type = str(data_type[0]).strip()

    multi = att_data['Multivalued'].unique().tolist()
    multi = str(multi[0]).strip()

    uom = att_data['UOM'].unique().tolist()
    uom = str(uom[0]).strip()

    allow = att_data['Allowed Values'].unique().tolist()
    allow = str(allow[0]).strip()
    
    sample = att_data['Sample Values'].unique().tolist()
    sample = str(sample[0]).strip()
    
#    prior = att_data['Priority'].unique().tolist()
#    prior = str(prior[0]).strip()
    
#    rank = att_data['Rank'].unique().tolist()
#    rank = str(rank[0]).strip()

    for row in att_data.itertuples():
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
                    hightouch_node = high_touch.at[row.Index, tax_col[0]]
                    hightouch_node = str(hightouch_node)
                    hightouch_node = hightouch_node.strip()
                    
                    if hightouch_node in category_name:
                        hightouch_att = high_touch.at[row.Index, att_col[0]]
                        hightouch_att = str(hightouch_att)
                        hightouch_att = hightouch_att.strip()
                
                        if hightouch_att == att_name:
                            hightouch_status = high_touch.at[row.Index, hightouch_col[0]]
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
                                       'Data Type' : data_type,
                                       'Multivalued' : multi,
                                       'UOM' : uom,
                                       'Allowed Values' : allow,
                                       'Sample Values' : sample,
#                                       'Priority' : prior,
#                                       'Rank' : rank,
                                       head: val
                                       }, ignore_index=True)
                    
    return temp_sheet


def data_out(final_df, high_touch, batch=''):
    # get rid of all 'nan' values in df / clean up final_df
    final_df.rename(columns={'RAW': 'Raw Value'})

    final_df = final_df.replace(np.nan, '', regex=True)
    final_df = final_df.replace('nan', '')
    
    final_df['High Touch'] = final_df['High Touch'].replace('', 'N')
    final_df['High Touch'] = final_df['High Touch'].str.upper()
    final_df = final_df.drop_duplicates() 
    
    if high_touch.empty == False:
        # clean up high_touch df

        # search for definition column
        def_col = [col for col in high_touch.columns if 'Definition' in col]

        hightouch_col = [col for col in high_touch.columns if 'High Touch' in col]
        if not hightouch_col:
            hightouch_col = [col for col in high_touch.columns if 'High-Touch' in col]
        
        tax_col = [col for col in high_touch.columns if 'Taxonomy' in col]
        if not tax_col:
            tax_col = [col for col in high_touch.columns if 'Node Name' in col]
        
        att_col = [col for col in high_touch.columns if 'Attribute Name' in col]
        if not att_col:
            att_col = [col for col in high_touch.columns if 'Attribute' in col]

        sample_col = [col for col in high_touch.columns if 'Sample' in col]

        if not def_col:
            high_touch = high_touch[[tax_col[0], att_col[0], sample_col[0], hightouch_col[0]]]    
        elif not sample_col:
            high_touch = high_touch[[tax_col[0], att_col[0], def_col[0], hightouch_col[0]]]    
        else:    
            high_touch = high_touch[[tax_col[0], att_col[0], def_col[0], sample_col[0], hightouch_col[0]]]    

        high_touch = high_touch.drop_duplicates()
        
    outfile = 'C:/Users/xcxg109/NonDriveFiles/Audit_Buildsheet_'+str(batch)+'.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter', options={'strings_to_urls': False})
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
                'Data Type', 'Multivalued', 'UOM', 'Sample Values', 'Allowed Values', \
               # 'Priority', 'Rank', 
                'High Touch', 'Attribute Name', 'Value', 'Unit', 'RAW', 'Source', 'Link', \
                'Image', 'Decision Log']
buildsheet_df = pd.DataFrame(columns = column_names)

for file in file_list:
    filename = os.fsdecode(file)
    filename = filename.lower()
    # read in the sheet name and parse for the category ID and name

    if 'high touch' in filename or 'high_touch' in filename:
        xls = pd.ExcelFile(file)
        hightouch_df = pd.read_excel(xls, 'Schema')
#        hightouch_df = hightouch_df.drop([hightouch_df.index[0], hightouch_df.index[1], hightouch_df.index[2]])
        
        print('FOUND: High Touch file')

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
        main_df = pd.read_excel(filename, header=None)
 #       main_df.to_csv('C:/Users/xcxg109/NonDriveFiles/pre.csv')
        
        cat_ID = main_df[0].unique().tolist()
        cat_ID = cat_ID[11]
        
        cat_name = gws.gws_q(ws_category, 'cat.id', cat_ID)
        cat_name = cat_name['name'].unique()
        cat_name = cat_name[0].strip()

        # flip buildsheet read-in to get it closer to our final format
        main_df = main_df.T 
        main_df[0].fillna(method='ffill', inplace=True)
        main_df[1].fillna(method='ffill', inplace=True)
        main_df[2].fillna(method='ffill', inplace=True)
        main_df[3].fillna(method='ffill', inplace=True)
        main_df[4].fillna(method='ffill', inplace=True)
        main_df[5].fillna(method='ffill', inplace=True)
        main_df[6].fillna(method='ffill', inplace=True)
        main_df[7].fillna(method='ffill', inplace=True)
        main_df[8].fillna(method='ffill', inplace=True)
        main_df[9].fillna(method='ffill', inplace=True)
 
        main_df.dropna(how='all', axis=1)
                
        # determine number of SKUs = number of times to iterateg
        skus = len(main_df.columns)
        skus = skus - 11     # remove non-SKU columns from count
        
        print('{}. {} : {} skus '.format(file_count, cat_name, skus))

        # set starting column for reading first SKU data
        count = 11
        
        for sku in range(skus):
            sku_time = time.time()
            # create unique df for each SKU
            sku_df = main_df[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, count]]
            sku_df.columns = ['Attribute_Name', 'Attribute_ID', 'Definition', 'Data Type', 'Multivalued', \
                              'UOM', 'Allowed Values', 'Sample Values', 'Priority', 'Rank', 'Header', 'Value']
            
            # separate SKU data into separate df and remove from attribute data
            meta = sku_df[sku_df['Attribute_Name'] == 'Attribute Name:']
            meta = meta[['Header', 'Value']]    # drop rows 0 ('Category ID') and 1 ('Attribute:')

            sku_df = sku_df[sku_df['Attribute_Name'] != 'Attribute Name:']        
 
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