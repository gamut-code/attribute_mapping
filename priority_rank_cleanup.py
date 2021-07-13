# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 22:04:58 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd
import time

def data_out(df):
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/ALLTIMEHIGH_FINAL_Rankings_WED.xlsx'
    
    df = df.sort_values(['GWS_Leaf_Node_ID', 'New_Rank', 'GWS_Attribute_Name'], ascending=[True, True, True])

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

start_time = time.time()

df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/wedmarch24final.csv')
#df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/rank_test_MON.csv')

df = df.drop_duplicates(subset=['Blue-PIM L3 Name', 'Blue-PIM L3 ID', 'STEP_Attr_ID', \
        'STEP Attribute Name', 'GWS_Leaf_Node_ID', 'GWS_Attr_ID', 'GWS_Leaf_Node_Name'],\
                        keep='first')
    
cats = df['GWS_Leaf_Node_ID'].unique().tolist()
print('Number of categories = ', len(cats))
count = 1

for cat in cats:
    print('{} : {}'.format(count, len(cats)))
    
    temp_df = df.loc[df['GWS_Leaf_Node_ID']== cat]
    
    temp_df = temp_df.sort_values(by=['rank'], ascending=[True])
    temp_df = temp_df[temp_df['GWS_Attr_ID'].notna()]

    table_df = temp_df[temp_df['rank'].notna()]
    
    table_count = 1
    table_df = table_df[['GWS_Attr_ID', 'rank']]
    
    for row in table_df.itertuples():
        table_df.at[row.Index, 'New_Rank'] = table_count

        table_count += 1

    temp_df = temp_df.merge(table_df, how="outer", on=['GWS_Attr_ID'])
    temp_df = temp_df.drop_duplicates()    

    final_df = pd.concat([final_df, temp_df], axis=0, sort=False)
    
    count += 1
    
data_out(final_df)
    
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
