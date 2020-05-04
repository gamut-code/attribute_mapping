# -*- coding: utf-8 -*-
"""
Created on Sat Apr  4 19:10:27 2020

@author: xcxg109
"""
import random

x = 146000

sku_list = [random.randrange(1, 50, 1) for i in range(x)] 

print('sku_list length =', len(sku_list))
if len(sku_list)>30000:
    num_lists = round(len(sku_list)/30000, 0)
    num_lists = int(num_lists)
    
    if num_lists == 1:
        num_lists = 2
        
    print('running SKUs in {} batches'.format(num_lists))
    
    size = round(x/num_lists, 0)
    size = int(size)

    div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

    for k  in range(0, len(div_lists)):
        gamut_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
        print('batch {}: SKUs = {}'.format(k, len(div_lists[k])))
else:
    print('SKUs = {}'.format(len(sku_list)))    