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


moist = PostgresDatabase_GWS()


# no need for an open connection,
# as we're only doing a single query
#engine.dispose()


#def test_query(search, k):
test_q="""
   WITH RECURSIVE tax AS (
                SELECT  id,
            name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
                FROM    taxonomy_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false

                UNION ALL

                SELECT  category.id,
            category.name,
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM Terminal Node Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({k} = ANY(tax.ancestors))
        
    WHERE tprod.deleted = 'f'
""".format(k=7903)


start_time = time.time()
print('working...')

gws_df = moist.query(test_q)


outfile = Path(settings.directory_name)/"test.xlsx"
gws_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
