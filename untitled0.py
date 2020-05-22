# -*- coding: utf-8 -*-
"""
Created on Wed May 20 21:44:43 2020

@author: xcxg109
"""

import pandas as pd
import io
import requests

uom_df = pd.DataFrame()

uom_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_goupings.csv'
uom_list_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_data_sheet.csv'


data_file = requests.get(uom_groups_url).content
groups_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

data_file = requests.get(uom_list_url).content
uom_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

uom_list = uom_df['Abbreviation (exact)'].to_list()
uom_set = set(uom_list)