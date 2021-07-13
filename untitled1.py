# -*- coding: utf-8 -*-
"""
Created on Fri Jun  4 09:12:26 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
import settings_NUMERIC as settings
import time
from GWS_query import GWSQuery

gws = GWSQuery()
pd.options.mode.chained_assignment = None

prod_ids = """
    SELECT id
    
    FROM {}
    
    WHERE "gtPartNumber" IN ({})
"""

all_vals = """
    SELECT COUNT(tprod_value.id)
	
    FROM taxonomy_product_attribute_value tprod_value
    
    INNER JOIN taxonomy_attribute tax_att
    	ON tax_att.id = tprod_value.{}
	
    WHERE tprod_value.deleted = false
    	AND tax_att.deleted = false
        
        AND tprod_value."productId" IN ({})
        """

print ('Choose SKU list to transpose')

file_data = settings.get_file_data()
sku_list = [row[0] for row in file_data[1:]]

prod_id_list = pd.DataFrame()
all_num = 0

start_time = time.time()

sku_list = [each_string.upper() for each_string in sku_list]
    
if len(sku_list)>4000:
    num_lists = round(len(sku_list)/4000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    print('running SKUs in {} batches'.format(num_lists))

    size = round(len(sku_list)/num_lists, 0)
    size = int(size)

    div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

    for k  in range(0, len(div_lists)):
        gr_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
        temp_df = gws.gws_q(prod_ids, 'taxonomy_product',  gr_skus)
        prod_id_list = pd.concat([prod_id_list, temp_df], axis=0, sort=False)
        
        prod_ID = prod_id_list['id'].unique().tolist()
        products = ", ".join(str(i) for i in prod_ID)
    
        temp_num = gws.gws_q(all_vals, '"attributeId"', products)
        
        temp_num = temp_num['count'][0]

        print ('Round {} - {} SKUs: \nTEMP_num = {}'.format(k+1, len(div_lists[k]), temp_num))        
        all_num = all_num + temp_num
        print ('Round {}: \nall_num = {}'.format(k+1, all_num))

#else:
#    gr_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
#    prod_id_list = gws.gws_q(prod_ids, 'taxonomy_product', gr_skus)

#    prod_ID = prod_id_list['id'].unique().tolist()
#    products = ", ".join(str(i) for i in prod_ID)

#    gov_num = gws.gws_q(numeric_vals, 'tax_att."dataType"', products)
#    gov_allow_text = gws.gws_q(allowed_vals, "'text'", products)
#    gov_allow_num = gws.gws_q(allowed_vals, "'number'", products)

#    print ('gov_num = {}\ngov_allow_text = {}\ngov_allow_num = {}'.format(gov_num['count'][0], gov_allow_text['count'][0], gov_allow_num['count'][0]))

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
