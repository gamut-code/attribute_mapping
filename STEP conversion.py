# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 11:50:27 2020

@author: xcxg109
"""

import pandas as pd
import numpy as np
import settings_NUMERIC as settings
import time

pd.options.mode.chained_assignment = None

def get_att_values():
    """ read in externally generated file of all attribute values at the L1 level. file format exported from
    teradata SQL assistant as tab delimited text """
   
    filename = settings.choose_file()

    delim = ','
    
    """ ignore errors on import lines (message will print when loading) """
    df = pd.read_csv(filename, delimiter=delim, error_bad_lines=False)
    
    df['Count'] = 1

    return df


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def process_vals(info_df, process_df, updatedVal_col):
    # transpose values for each chunk of the dataframe
    # iterate through the info_df and if we already have an entry for the SKU, add the attribute ID to that entry
    # otherwise, create a new entry with SKU and attribute ID
    
    for row in info_df.itertuples():
        att_name = info_df.at[row.Index, 'Grainger_Attr_ID']
        att_name = str(att_name)
        att_name = att_name.strip()

        att_value = info_df.at[row.Index, updatedVal_col[0]]
        att_value = str(att_value)
        att_value = att_value.strip()
    
        sku = info_df.at[row.Index, 'Grainger_SKU']
        sku = str(sku)
        sku = sku.strip()

        temp_df = process_df.loc[process_df['<ID>']== sku]

        if temp_df.empty == False:
            temp_df[att_name] = att_value
            process_df = process_df.combine_first(temp_df)
         
        else:
            process_df = process_df.append({'<ID>': sku, att_name: att_value}, ignore_index=True)

    return process_df


def data_out(final_df, node_name, batch=''):
    final_df = final_df.sort_values(['<ID>'], ascending=[True])
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node_name)+'_'+str(batch)+'_STEP-upload.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    final_df.to_excel (writer, sheet_name="STEP upload", startrow=0, startcol=0, index=False)
    worksheet = writer.sheets['STEP upload']

    col_widths = get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width)

    writer.save()


# read in data
print('Choose file for STEP upload conversion')
df = get_att_values()

print('working...')
start_time = time.time()

node_name = df['Segment_Name'].unique()
node_name = node_name[0]

temp_df = pd.DataFrame()

# filter where column names and values can vary
update_col = [col for col in df.columns if 'Update?' in col]
if not update_col:
    update_col = [col for col in df.columns if 'Y/N' in col]
if not update_col:
    update_col = [col for col in df.columns if 'Use Update' in col]

updatedVal_col = [col for col in df.columns if 'Updated Value' in col]
if not updatedVal_col: 
    updatedVal_col = [col for col in df.columns if 'Updated_Value' in col]

if not updatedVal_col: 
    print('updated value column EMPTY!')
else:   
    print('updated value column = ', updatedVal_col[0])


# filter where we have a specific column name and variable
filter_vals = ['Y', 'y']
df = df[df[update_col[0]].isin(filter_vals)]

df = df[['Grainger_SKU','Grainger_Attr_ID', updatedVal_col[0]]]

df['Grainger_Attr_ID'] = df['Grainger_Attr_ID'].astype(str)
df['Grainger_Attr_ID'] = df['Grainger_Attr_ID']+'_ATTR'

column_names = df['Grainger_Attr_ID'].unique().tolist()
column_names = list(map(str, column_names))
column_names.insert(0, '<ID>')

step_df = pd.DataFrame(columns=column_names)
print('transposing {} values', len(df))

if len(df) > 3000:
    count = 1

    # split into multiple dfs of 4K rows, creating at least 2
    num_lists = round(len(df)/3000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2

    print('processing values in {} batches'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        # step_df at this point is an empty shell used to transpose SKUs and attribute data
        step_df = process_vals(object, step_df, updatedVal_col)
#        step_df = pd.concat([step_df, temp_df], axis=0, sort=False)

        count += 1
                    
# if original df < 4K rows, process the entire thing at once
else:
    step_df = process_vals(df, step_df, updatedVal_col)

if len(step_df) > 50000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(step_df)/45000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(step_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, node_name, count)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(step_df, node_name)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))