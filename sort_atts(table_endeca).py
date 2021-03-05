# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 11:03:10 2021

@author: xcxg109
"""

import pandas as pd
import file_data_GWS as fd

def data_out(df):
    
    outfile = 'C:/Users/xcxg109/NonDriveFiles/Priority_Rankings.xlsx'
    
    df = df.sort_values(['PIM Leaf Node Name', 'rank', 'PIM Attribute Name'], ascending=[True, True, True])

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

    
df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/required_optional_V2.csv')

final_df = pd.DataFrame()

cat_list = df['STEP Category IDs'].unique().tolist()

for cat in cat_list:
    print('cat = ', cat)
    temp_df = df[df['STEP Category IDs'] == cat]

    table_df = temp_df[temp_df['Table Ranking'] != 0]
    table_df = table_df.sort_values(['Table Ranking'])
    table_count = 1
    
    table_df = table_df[['STEP Attribute IDs', 'rank']]

    for row in table_df.itertuples():
        table_df.at[row.Index, 'rank'] = table_count

        table_count += 1        

    temp_df = temp_df.drop('rank', axis=1)
    temp_df = temp_df.merge(table_df, how="outer", on=['STEP Attribute IDs'])

    temp_df['rank'] = temp_df['rank'].fillna(0)

    endeca_df = temp_df[temp_df['rank'] == 0]
    endeca_df = endeca_df[endeca_df['Endeca Ranking'] != 0]
    endeca_df = endeca_df.sort_values(['Endeca Ranking'])

#    endeca_df = endeca_df[['STEP Attribute IDs', 'rank']]
        
    for row in endeca_df.itertuples():
        endeca_df.at[row.Index, 'rank'] = table_count

        table_count += 1        
        
        temp_df = endeca_df.combine_first(temp_df)
    
    final_df = pd.concat([final_df, temp_df], axis=0, sort=False)
    
    data_out(final_df)