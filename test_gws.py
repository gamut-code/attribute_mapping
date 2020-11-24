 # -*- coding: utf-8 -*-

import time


"""
Spyder Editor

This is a temporary script file.
"""

#ORIGINAL Gamut Test
#from postgres_client import PostgresDatabase
#db = PostgresDatabase()

#1.5 ADMIN Gamut Test
#from postgres_gamut_15 import PostgresDatabase_15
#moist = PostgresDatabase_15()

#GWS New Workstation Test
from postgres_GWS import PostgresDatabase_GWS
from pathlib import Path
import settings_NUMERIC as settings
from GWS_query import GWSQuery
gws = GWSQuery()
moist = PostgresDatabase_GWS()


# no need for an open connection,
# as we're only doing a single query
#engine.dispose()


#def test_query(search, k):
test_q="""
       SELECT *
       
       FROM taxonomy_product_backfeed
       
       WHERE {} IN ({})
"""


start_time = time.time()
print('working...')

#gws_df = moist.query(test_q)
gws_df  = gws.gws_q(test_q, 'taxonomy_product_backfeed.value', 4000)

outfile = Path(settings.directory_name)/"test.xlsx"
gws_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
