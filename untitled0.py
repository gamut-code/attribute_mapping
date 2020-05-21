# -*- coding: utf-8 -*-
"""
Created on Wed May 20 21:44:43 2020

@author: xcxg109
"""

import pandas as pd
import io
import requests

uom_df = pd.DataFrame()

url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_data_sheet.csv'
uom_list = list()
uom_set = set()

data_file = requests.get(url).content
uom_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

#uom_list = uom_df['uoms_in_group'].unique()
for row in uom_df.itertuples():    
    uom_list = uom_df['uoms_in_group'].str.split(';',expand=True)
    print('ROW {} {}'.format(row, uom_list))
#res_df = uom_df[uom_df['uoms_in_group'].str.contains('in.')]
