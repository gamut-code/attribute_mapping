# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 17:12:17 2020

@author: xcxg109
"""

import pandas as pd
import numpy as np
import WS_query_code as q
from re import search


def match_lovs(lov_df, lov_list, attr_id):
    """compare the 'Grainger_Attr_ID' column against our list of LOVs"""
    
    values_list = list()

    if attr_id in lov_list:
        temp_df = lov_df.loc[lov_df['AttributeID']== attr_id]
        values_list = temp_df['Value'].tolist()

    return values_list


def process_LOV_vals(df, row):
    """ clean up the sample values column """
    
    search_string = ''
     
    LOV_val = str(row.Value)
    
    if '"' in LOV_val:
        search_string = search_string+'; '+'"'

    if 'in.' in LOV_val:
        search_string = search_string+'; '+'in.'

    if 'ft.' in LOV_val:
        search_string = search_string+'; '+'ft.'

    if 'yd.' in LOV_val:
        search_string = search_string+'; '+'yd.'

    if 'fl.' in LOV_val:
        search_string = search_string+'; '+'fl.'

    if 'oz.' in LOV_val:
        search_string = search_string+'; '+'oz.'

    if 'pt.' in LOV_val:
        search_string = search_string+'; '+'pt.'

    if 'qt.' in LOV_val:
        search_string = search_string+'; '+'qt.'

    if 'kg.' in LOV_val:
        search_string = search_string+'; '+'kg.'

    if 'gal.' in LOV_val:
        search_string = search_string+'; '+'gal.'

    if 'lb.' in LOV_val:
        search_string = search_string+'; '+'lb.'

    if 'cu.' in LOV_val:
        search_string = search_string+'; '+'cu.'
        
    if 'cf.' in LOV_val:
        search_string = search_string+'; '+'cf.'

    if 'sq.' in LOV_val:
        search_string = search_string+'; '+'sq.'
        
    if '° C' in LOV_val or '°C' in LOV_val:
        search_string = search_string+'; '+'° C'
        
    if '° F' in LOV_val or '°F' in LOV_val:
        search_string = search_string+'; '+'° F'

    if 'deg.' in LOV_val:
        search_string = search_string+'; '+'deg.'
        
    if 'ga.' in LOV_val:
        search_string = search_string+'; '+'ga.'

    if 'min.' in LOV_val:
        search_string = search_string+'; '+'min.'

    if 'sec.' in LOV_val:
        search_string = search_string+'; '+'sec.'

    if 'hr.' in LOV_val:
        search_string = search_string+'; '+'hr.'

    if 'wk.' in LOV_val:
        search_string = search_string+'; '+'wk.'

    if 'mo.' in LOV_val:
        search_string = search_string+'; '+'mo.'

    if 'yr.' in LOV_val:
        search_string = search_string+'; '+'yr.'

    if 'µ' in LOV_val:
        search_string = search_string+'; '+'µ'

    if 'dia.' in LOV_val:
        search_string = search_string+'; '+'dia.'

    search_string = search_string[2:]

    df.at[row.Index,'Potential_UOMs'] = search_string
    
    return df

# get uom list
filename = 'C:/Users/xcxg109/NonDriveFiles/reference/UOM_data_sheet.csv'
uom_df = pd.read_csv(filename)
# create df of the lovs and their concat values

filename = 'C:/Users/xcxg109/NonDriveFiles/reference/LOV_Categories.csv'
lov_df, lov_list = q.get_LOVs(filename)

lov_df['Potential_UOMs'] = ''

for row in lov_df.itertuples():
    lov_df = process_LOV_vals(lov_df, row)

lov_df = lov_df.replace(r'^\s*$', np.NaN, regex=True)
lov_df = lov_df.sort_values(['Potential_UOMs'], ascending=[True])

outfile = 'C:/Users/xcxg109/NonDriveFiles/reference/LOV_UOMs.xlsx'  
writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
workbook  = writer.book


lov_df.to_excel (writer, sheet_name="LOVs", startrow=0, startcol=0, index=False)

worksheet = writer.sheets['LOVs']

layout = workbook.add_format()
layout.set_text_wrap('text_wrap')
layout.set_align('left')

worksheet.set_column('B:B', 120, layout)
worksheet.set_column('C:C', 30, layout)

writer.save()