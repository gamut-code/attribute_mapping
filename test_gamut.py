 # -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd

#ORIGINAL Gamut Test

from postgres_ORIGINAL_gamut import PostgresDatabase
db = PostgresDatabase()



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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "Gamut_PIM_Path"
        , tax.ancestors[1] as "Gamut_Category_ID"  
        , tax.ancestor_names[1] as "Gamut_Category_Name"
        , tprod."categoryId" AS "Gamut_Node_ID"
        , tax.name as "Gamut_Node_Name"
        , tprod."gtPartNumber" as "Gamut_SKU"
        , tprod."supplierSku" as "Grainger_SKU"
        , tax_att.id as "Gamut_Attr_ID"
        , tax_att.name as "Gamut_Attribute_Name"
        , tax_att.description as "Gamut_Attribute_Definition"
        , tprodvalue.value as "Original Value"
        , tprodvalue."valueNormalized" as "Normalized Value"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE {term} IN ({k})
        """(term='tax_att."categoryId"', k=8786)

gamut_df = db.query(test_q)

gamut_df.to_csv('F:\CGabriel\Grainger_Shorties\OUTPUT\gamut_uom.csv')