# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 11:50:27 2020

@author: xcxg109
"""

import pandas as pd
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


def data_out(final_df):
    final_df = final_df.sort_values(['<ID>'], ascending=[True])
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/STEP-upload.xlsx'  
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

# filter where column names and values can vary
update_col = [col for col in df.columns if 'Update?' in col]
if not update_col:
    update_col = [col for col in df.columns if 'Y/N' in col]
if not update_col:
    update_col = [col for col in df.columns if 'Use Update' in col]

# filter where we have a specific column name and variable
filter_vals = ['Y', 'y']
df = df[df[update_col[0]].isin(filter_vals)]

df = df[['Grainger_SKU','Grainger_Attr_ID','Updated Value']]

df['Grainger_Attr_ID'] = df['Grainger_Attr_ID'].astype(str)
df['Grainger_Attr_ID'] = df['Grainger_Attr_ID']+'_ATTR'

column_names = df['Grainger_Attr_ID'].unique().tolist()
column_names = list(map(str, column_names))
column_names.insert(0, '<ID>')

step_df = pd.DataFrame(columns=column_names)
print('transposing {} values', len(df))

for row in df.itertuples():
    att_name = df.at[row.Index, 'Grainger_Attr_ID']
    att_name = str(att_name)
    att_name = att_name.strip()

    att_value = df.at[row.Index, 'Updated Value']
    att_value = str(att_value)
    att_value = att_value.strip()
    
    sku = df.at[row.Index, 'Grainger_SKU']
    sku = str(sku)
    sku = sku.strip()

    temp_df = step_df.loc[step_df['<ID>']== sku]

    if temp_df.empty == False:
        temp_df[att_name] = att_value
        step_df = step_df.combine_first(temp_df)
         
    else:
        step_df = step_df.append({'<ID>': sku, att_name: att_value}, ignore_index=True)

data_out(step_df)
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))