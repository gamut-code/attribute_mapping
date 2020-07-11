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
from postgres_TOOLBOX_gws import PostgresDatabase_toolbox
from pathlib import Path
import settings


moist = PostgresDatabase_toolbox()


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
                        tax.ancestors || category."parentId",
                        tax.ancestor_names || parent_category.name
                FROM taxonomy_category as category
                JOIN tax on category."parentId" = tax.id
                JOIN taxonomy_category parent_category on category."parentId" = parent_category.id
                WHERE   category.deleted = false 
            )

            SELECT
                tprod."gtPartNumber" as "Grainger_SKU"
                , array_to_string(tax.ancestor_names || tax.name,' > ') as "tax_path"
                , tprod."categoryId" as "PIM Node ID"

            FROM taxonomy_product tprod

            INNER JOIN tax
                ON tax.id = tprod."categoryId"
                -- AND (10006 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

            WHERE {term} IN ({k})
""".format(term='tprod."categoryId"', k=849)


start_time = time.time()
print('working...')

toolbox_df = moist.query(test_q)


outfile = Path(settings.directory_name)/"test.xlsx"
toolbox_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
