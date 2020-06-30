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
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM Terminal Node Path"
        , tprod."categoryId" AS "PIM Terminal Node ID"
        , tprod."gtPartNumber" as "Gamut Part Number"
        , tprod."supplierSku" as "Grainger SKU"
        , tax_att.name as "Attribute Name"
        , tax_att.description as "Attribute Definition"
        , tax_att."unitGroupId" AS "UOM ID"
        , tax_att."dataType" as "Data Type"
        , tprodvalue.value as "Original Value"
        , tprodvalue.unit as "UOM"
        , tprodvalue."valueNormalized" as "Normalized Value"
        , tprodvalue."unitNormalized" as "UOM"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
	AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tprod.deleted = 'f'
          AND {term} IN ({k})
""".format(term='tprod."categoryId"', k=633)


start_time = time.time()
print('working...')

toolbox_df = moist.query(test_q)


outfile = Path(settings.directory_name)/"test.xlsx"
toolbox_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
