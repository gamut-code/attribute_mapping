# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019
"""

import file_data_GWS as fd
import settings
import hierarchy_step as hier
import time


print('working....')
start_time = time.time()

grainger_df, search_level = hier.generate_data()

quer='HIER'

fd.data_out(settings.directory_name, grainger_df, search_level, quer)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))