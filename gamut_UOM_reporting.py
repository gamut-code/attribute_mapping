 # -*- coding: utf-8 -*-
"""
Created on Thurs July 02 2020

@author: xcxg109
"""

import pandas as pd

from gamut_ORIGINAL_query import GamutQuery
from queries_PIM import gamut_attr_query, gamut_attr_values
import settings
import time

gamut = GamutQuery()

def data_in(directory_name):
    uom_group = input('Input UOM Group ID or hit ENTER to read from file: ')

    if uom_group != "":
        uom_group = [uom_group]
        return uom_group

    else:
        file_data = settings.get_file_data()

        uom_group = [int(row[0]) for row in file_data[1:]]
        return uom_group


gamut_df = pd.DataFrame()
all_vals = pd.DataFrame()


uom_data = data_in(settings.directory_name)

start_time = time.time()
print('working...')

for k in uom_data:
    print('\nUOM group = ', k)
    gamut_df = gamut.gamut_q(gamut_attr_query, 'tax_att."unitGroupId"', k)
    
    if gamut_df.empty == False:
        atts = gamut_df['Gamut_Attr_ID'].to_list()

        for attribute in atts:
            temp_df = gamut.gamut_q(gamut_attr_values, 'tax_att.id', attribute)
            temp_df['Count'] = 1

            if temp_df.empty == False:
                vals = pd.DataFrame(temp_df.groupby(['Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Normalized Unit'])['Count'].sum())
                vals = vals.reset_index()
#                vals = vals.sort_values(by=['Count'], ascending=[False])

                temp_df['UOMs Present'] = '; '.join(item for item in vals['Normalized Unit'] if item)

                all_vals = pd.concat([all_vals, temp_df], axis=0)

        if all_vals.empty == False:
            all_vals = all_vals[['Gamut_Attr_ID', 'UOMs Present']]
            all_vals = all_vals.drop_duplicates(subset=['Gamut_Attr_ID'])

            gamut_df = pd.merge(gamut_df, all_vals, on=['Gamut_Attr_ID'])  

        columnsTitles = ['Gamut_PIM_Path', 'Gamut_Category_ID', 'Gamut_Category_Name', 'Gamut_Node_ID', \
                         'Gamut_Node_Name', 'Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Gamut_Attribute_Definition', \
                         'Unit_Group_ID', 'UOM_Kind', 'UOMs Present']
        gamut_df = gamut_df.reindex(columns=columnsTitles)

        filename= 'F:\CGabriel\Grainger_Shorties\OUTPUT\gamut_UOM_group_' + str(k) + '.xlsx' 

        gamut_df.to_excel(filename)
    else:
        print('\nEMPTY DATAFRAME\n')

    print("--- {} seconds ---".format(round(time.time() - start_time, 2)))