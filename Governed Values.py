# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 15:16:46 2021

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


numeric_vals = """
    SELECT COUNT(tprod_value.id)
	
    FROM taxonomy_product_attribute_value tprod_value

    INNER JOIN taxonomy_attribute tax_att
    	ON tax_att.id = tprod_value."attributeId"
	
    WHERE tprod_value.deleted = false
    	AND tax_att.deleted = false
        AND {} = 'number'
        
        AND tprod_value."productId" IN ({})
"""


allowed_vals = """
    SELECT COUNT(tprod_value)
	
    FROM taxonomy_product_attribute_value tprod_value
    
    INNER JOIN taxonomy_attribute tax_att
    	ON tax_att.id = tprod_value."attributeId"
	
    WHERE tprod_value.deleted = false
    	AND tax_att.deleted = false
        AND tax_att."allowedValues" IS NOT NULL
        AND tax_att."dataType" = {}    
        
        AND tprod_value."productId" IN ({})
"""


print ('Choose SKU list to transpose')

prod_id_list = pd.DataFrame()
gov_num = 0
gov_allow_text = 0
gov_allow_num = 0

file_data = settings.get_file_data()
sku_list = [row[0] for row in file_data[1:]]

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
    
        temp_gov_num = gws.gws_q(numeric_vals, 'tax_att."dataType"', products)
        temp_gov_allow_text = gws.gws_q(allowed_vals, "'text'", products)
        temp_gov_allow_num = gws.gws_q(allowed_vals, "'number'", products)
        
        temp_gov_num = temp_gov_num['count'][0]
        temp_gov_allow_text = temp_gov_allow_text['count'][0]
        temp_gov_allow_num = temp_gov_allow_num['count'][0]

        print ('Round {} - {} SKUs: \nTEMPgov_num = {}\nTEMPgov_allow_text = {}\nTEMPgov_allow_num = {}'.format(k+1, len( div_lists[k]), temp_gov_num, temp_gov_allow_text, temp_gov_allow_num))        
        gov_num = gov_num + temp_gov_num
        gov_allow_text = gov_allow_text + temp_gov_allow_text
        gov_allow_num = gov_allow_num + temp_gov_allow_num
        print ('Round {}: \ngov_num = {}\ngov_allow_text = {}\ngov_allow_num = {}'.format(k+1, gov_num, gov_allow_text, gov_allow_num))

else:
    gr_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    prod_id_list = gws.gws_q(prod_ids, 'taxonomy_product', gr_skus)

    prod_ID = prod_id_list['id'].unique().tolist()
    products = ", ".join(str(i) for i in prod_ID)

    gov_num = gws.gws_q(numeric_vals, 'tax_att."dataType"', products)
    gov_allow_text = gws.gws_q(allowed_vals, "'text'", products)
    gov_allow_num = gws.gws_q(allowed_vals, "'number'", products)

    print ('gov_num = {}\ngov_allow_text = {}\ngov_allow_num = {}'.format(gov_num['count'][0], gov_allow_text['count'][0], gov_allow_num['count'][0]))

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))