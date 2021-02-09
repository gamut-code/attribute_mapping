# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 15:56:20 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
import os
import shutil
#from os import listdir
#from os.path import isfile, join
#from tkinter import Tk
from tkinter.filedialog import askdirectory 
#from tkinter.filedialog import askopenfilename
import glob


gws = GWSQuery()

ws_category="""
    SELECT cat.name

    FROM    taxonomy_category as cat
    
    WHERE {} IN ({})
"""

##buildsheet_df = create_buildsheet(meta, sku_df, hightouch_df, buildsheet_df, cat_ID, cat_name)

def create_buildsheet(metadata, sku_data, high_touch, build_df, category_ID, category_name):
    # transpose values for each chunk of the dataframe
    # iterate through the temp_df and if we already have an entry for the attribute, add row data to that entry
    # otherwise, create a new entry with attribute name and attribute ID

    # gather all the SKU-specific data from the rows of the meta df
    sku = metadata.loc[metadata['Header'] == 'Grainger Part Number', 'Value'].item()
    status = metadata.loc[metadata['Header'] == 'Status', 'Value'].item()
    noun = metadata.loc[metadata['Header'] == 'Primary Noun', 'Value'].item()
    supplier = metadata.loc[metadata['Header'] == 'Supplier', 'Value'].item()
    supplier_part = metadata.loc[metadata['Header'] == 'Supplier Part Number', 'Value'].item()
    manufacturer = metadata.loc[metadata['Header'] == 'Manufacturer', 'Value'].item()
    series = metadata.loc[metadata['Header'] == 'Series', 'Value'].item()
    mfr_part = metadata.loc[metadata['Header'] == 'Mfr Part Number', 'Value'].item()

    for row in sku_data.itertuples():
        att_ID = sku_data.at[row.Index, 'Attribute_ID']
        att_ID = str(att_ID)
        att_ID = att_ID.strip()        
        
        att_name = sku_data.at[row.Index, 'Attribute_Name']
        att_name = str(att_name)
        att_name = att_name.strip()        

        head = sku_data.at[row.Index, 'Header']
        head = str(head)
        head = head.strip()

        val = sku_data.at[row.Index, 'Value']
        val = str(val)
        val = val.strip()

        temp_sheet = build_df.loc[build_df['Attribute_ID']== att_ID] 

        if temp_sheet.empty == False:
            hightouch_status = ''
            
            for row in high_touch.itertuples():
                hightouch_node = high_touch.at[row.Index, 'Taxonomy Node']
                hightouch_node = str(hightouch_node)
                hightouch_node = hightouch_node.strip()
                
                if hightouch_node in category_name:
                    print('FOUND CATEGORY', hightouch_node)
                    
                    hightouch_att = high_touch.at[row.Index, 'Attribute Name']
                    hightouch_att = str(hightouch_att)
                    hightouch_att = hightouch_att.strip()
            
                    if hightouch_att == att_name:
                        print('FOUND ATT NAME', hightouch_att)
                        
                        hightouch_status = high_touch.at[row.Index, 'High Touch']
                        hightouch_status = str(hightouch_status)
                        hightouch_status = hightouch_status.strip()
            
            print('HIGH TOUCH STATUS = ', hightouch_status)
                            
            temp_sheet[head] = val
            temp_sheet['High Touch'] = hightouch_status
            build_df = build_df.combine_first(temp_sheet)

        else:
           build_df = build_df.append({'Category ID': category_ID,
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
                    
    return build_df


def data_out(final_df):
        
    outfile = 'C:/Users/xcxg109/NonDriveFiles/Audit_Buildsheet.xlsx'  

        
print('Choose build sheet directory')
init_dir = 'R:/PM/Hsueh/Audit Template Test'

path = askdirectory(initialdir=init_dir)

os.chdir(path)
file_list = glob.glob('*.xlsx')

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

for file in file_list:
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
    
        # create a list of the attributes that we'll transpose
#        att_IDs = main_df.iloc[0].tolist()
#        att_IDs = [x for x in att_IDs if str(x) != 'nan']
        
#        cat_ID = att_IDs.pop(0)

        cat_name = gws.gws_q(ws_category, 'cat.id', cat_ID)
        cat_name = cat_name['name'].unique()
        cat_name = cat_name[0]

#        attributes = main_df.iloc[1].tolist()
#        attributes = [x for x in attributes if str(x) != 'nan']
#        attributes.remove('Attributes:')
    
#        header_row = 1
        
        # build dictionary of attribute : att_ID
#        values = {attributes[i]: att_IDs[i] for i in range(len(att_IDs))}         

        # flip buildsheet read-in to get it closer to our final format        
        main_df = main_df.T 
        main_df[0].fillna(method='ffill', inplace=True)
        main_df[1].fillna(method='ffill', inplace=True)
    
        # find all data that is not         
#        metadata = main_df[main_df[1] == 'Attributes:']

        # determine number of SKUs = number of times to iterateg
        skus = len(main_df.columns)
        skus = skus - 3     # remove non-SKU columns from count
        
        # set starting point for reading first SKU data
        count = 3
        
        for sku in range(skus):
            # create unique df for each SKU
            sku_df = main_df[[0, 1, 2, count]]
            sku_df.columns = ['Attribute_ID', 'Attribute_Name', 'Header', 'Value']
            
            # separate SKU data into separate df and remove from attribute data
            meta = sku_df[sku_df['Attribute_Name'] == 'Attributes:']
            meta = meta[['Header', 'Value']]    # drop rows 0 ('Category ID') and 1 ('Attribute:')

            sku_df = sku_df[sku_df['Attribute_Name'] != 'Attributes:']        
 
            temp_build_df = create_buildsheet(meta, sku_df, hightouch_df, buildsheet_df, cat_ID, cat_name)
            buildsheet_df = pd.concat([buildsheet_df, temp_build_df], axis=0, sort=False)
            
            #increment column count so metadata/data for the next sku is pulled
            count += 1
            
        data_out(buildsheet_df)