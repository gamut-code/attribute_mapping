# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import file_data_att as fd
import settings
import pandas as pd

"""CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
from gamut_query import GamutQuery
#from GWS_query import GWSQuery
""" """
from grainger_query import GraingerQuery
from queries_PIM import gamut_hier_query, grainger_basic_query, \
                        grainger_discontinued_query
import query_code as q
import time


"""CODE TO SWITCH BETWEEN 1.5 SYSTEM AND GWS"""
gamut = GamutQuery()
#gamut = GWSQuery()
""" """
gcom = GraingerQuery()


def gamut_data(grainger_df):
    sku_list = grainger_df['Grainger_SKU'].tolist()
    gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
    """CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
    gamut_df = gamut.gamut_q(gamut_hier_query, 'tprod."supplierSku"', gamut_skus)
#    gamut_df = gamut.gws_q(gamut_hier_query, 'tprod."supplierSku"', gamut_skus)
    """ """

    return gamut_df

def grainger_data(gamut_df):
    sku_list = gamut_df['supplierSku'].tolist()
    grainger_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', grainger_skus)
    return grainger_df


#determine whether or not to include discontinued items in the data pull
def skus_to_pull():
    """choose whether to included discontinued SKUs"""
    while True:
        try:
            sku_status = input("Include DISCOUNTINUED skus? ")
            if sku_status in ['Y', 'y', 'Yes', 'YES', 'yes']:
                sku_status = 'all'
                break
            elif sku_status in ['N', 'n', 'No', 'NO', 'no']:
                sku_status = 'filtered'
                break
        except ValueeError:
            print('Invalid search type')
        
    return sku_status


gamut_df = pd.DataFrame()
grainger_df = pd.DataFrame()


quer = 'HIER'
search_level = 'tax.id'

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()
    sku_status = skus_to_pull() #determine whether or not to include discontinued items in the data pull

search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')


if data_type == 'gamut_query':
    for k in search_data:
        
        """CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
        temp_df = gamut.gamut_q(gamut_hier_query, search_level, k)
#        temp_df = gamut.gws_q(gamut_hier_query, search_level, k)
        """ """

        gamut_df = pd.concat([gamut_df, temp_df], axis=0)
        if gamut_df.empty == False:
            grainger_df = grainger_data(gamut_df)
            fd.hier_data_out(settings.directory_name, grainger_df, gamut_df, quer, search_level)
        else:
            print('{} No SKUs in node'.format(k))

elif data_type == 'grainger_query':
    for k in search_data:
        if sku_status == 'filtered':
            temp_df = gcom.grainger_q(grainger_basic_query, search_level, k)
        elif sku_status == 'all':
            temp_df = gcom.grainger_q(grainger_discontinued_query, search_level, k)
        grainger_df = pd.concat([grainger_df, temp_df], axis=0)
        if grainger_df.empty == False:
            gamut_df = gamut_data(grainger_df)
            fd.hier_data_out(settings.directory_name, grainger_df, gamut_df, quer, search_level)
        else:
           print('All SKUs are R4, R9, or discontinued')       
           
elif data_type == 'sku':
    search_level = 'SKU'
    sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', sku_str)
    if grainger_df.empty == False:
        gamut_df = gamut_data(grainger_df)    
        if gamut_df.empty == False:
            gamut = 'yes'
            grainger_df = grainger_df.merge(gamut_df, how="left", on=["Grainger_SKU"])
            fd.data_out(settings.directory_name, grainger_df, quer, search_level)
    else:
        print('No SKU data for ', sku_str)

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
