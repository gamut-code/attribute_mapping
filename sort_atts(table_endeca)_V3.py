# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 11:03:10 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
import file_data_GWS as fd
import time

def data_out(df):
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/Priority_Rankings_THURS.xlsx'
    
    df = df.sort_values(['GWS_Leaf_Node_Name', 'rank', 'GWS_Attribute_Name'], ascending=[True, True, True])

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    df.to_excel (writer, sheet_name="STEP Attributes", startrow=0, startcol=0, index=False)
    worksheet1 = writer.sheets['STEP Attributes']
        
    col_widths = fd.get_col_widths(df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    writer.save()


final_df = pd.DataFrame()
endeca_only = pd.DataFrame()

df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/wed_hues.csv')
df = df[df['STEP_Attr_ID'] != '102079_ATTR']

start_time = time.time()

cat_list = df['Blue-PIM L3 ID'].unique().tolist()
count = 1

for cat in cat_list:
    print('{}. {}'.format(count, cat))
    temp_df = df[df['Blue-PIM L3 ID'] == cat]
    temp_df = temp_df.sort_values(by=['Table Ranking', 'Endeca Ranking'], ascending=[True, True])
    temp_df = temp_df.drop_duplicates(subset = 'STEP_Attr_ID', keep='first')

    table_df = temp_df[temp_df['Table Ranking'].notna()]
    
    if table_df.empty == True:
        endeca_only = pd.concat([endeca_only, temp_df], axis=0, sort=False)

    table_count = 1
        
    table_df = table_df[['STEP_Attr_ID', 'rank']]

    for row in table_df.itertuples():
        table_df.at[row.Index, 'rank'] = table_count

        table_count += 1        

    temp_df = temp_df.drop('rank', axis=1)
    temp_df = temp_df.merge(table_df, how="outer", on=['STEP_Attr_ID'])

    endeca_df = temp_df[temp_df['Endeca Ranking'].notna()]
    endeca_df = endeca_df[endeca_df['rank'].isna()]

    endeca_df = endeca_df.sort_values(['Endeca Ranking'])

    for row in endeca_df.itertuples():
        endeca_df.at[row.Index, 'rank'] = table_count

        table_count += 1        
        
        temp_df = endeca_df.combine_first(temp_df)
    
    final_df = pd.concat([final_df, temp_df], axis=0, sort=False)
    
    count += 1

data_out(final_df)
    
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
