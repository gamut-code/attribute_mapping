     # -*- coding: utf-8 -*-

import time


"""
Spyder Editor

This is a temporary script file.
"""

from postgres_GWS import PostgresDatabase_GWS
from pathlib import Path
import settings_NUMERIC as settings


moist = PostgresDatabase_GWS()

test_q="""
select
    cat.id as "WS_Node_ID"
    , cat.name as "WS_Category_Name"
    , back."stepNodeId" as "STEP_Category_ID"
    , back."syncedOn" ::date
    , tprod.id as "Product_ID"
    , tprod."gtPartNumber" as "SKU"
    
FROM taxonomy_product_backfeed as back

LEFT JOIN taxonomy_product as tprod
    ON tprod.id = back."taxonomyProductId"
    
LEFT JOIN taxonomy_category as cat
    ON cat.id = tprod."categoryId"

WHERE back."syncedOn" >= '2021-01-05' ::date
  
""".format(k=7903)


start_time = time.time()
print('working...')

gws_df = moist.query(test_q)

#gws_df[['date','time']] = df.Name.str.split(expand=True) 



outfile = Path(settings.directory_name)/"product_backfeeds.xlsx"
gws_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
