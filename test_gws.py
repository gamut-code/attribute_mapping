 # -*- coding: utf-8 -*-

import time


"""
Spyder Editor

This is a temporary script file.
"""


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
 --pulls Raw and Normalized values for WS Attributes

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
  array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
, tax.ancestors[1] as "WS_Category_ID"  
, tax.ancestor_names[1] as "WS_Category_Name"
, tprod."categoryId" AS "WS_Node_ID"
, tax.name as "WS_Node_Name"
, tprod."gtPartNumber" as "WS_SKU"
, pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
, pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
, tax_att.id as "WS_Attr_ID"
, tprodvalue.id as "WS_Attr_Value_ID"
, tax_att."multiValue" as "Multivalue"
, tax_att."dataType" as "Data_Type"
, tax_att."numericDisplayType" as "Numeric_Display_Type"
--        , tax_att.description as "WS_Attribute_Definition"
, tax_att.name as "WS_Attribute_Name"
, tprodvalue."valueNormalized" as "Normalized_Value"
, tprodvalue."unitNormalized" as "Normalized_Unit"

FROM  taxonomy_product tprod

INNER JOIN tax
ON tax.id = tprod."categoryId"
AND tprod.status = 3

INNER JOIN taxonomy_attribute tax_att
ON tax_att."categoryId" = tprod."categoryId"
AND tax_att.deleted = 'false'

INNER JOIN  taxonomy_product_attribute_value tprodvalue
ON tprod.id = tprodvalue."productId"
AND tax_att.id = tprodvalue."attributeId"
AND tprodvalue.deleted = 'false'
AND tax_att."multiValue" = 'true'
        
LEFT OUTER JOIN pi_mappings
ON pi_mappings.gws_attribute_ids[1] = tax_att.id
AND pi_mappings.gws_category_id = tax_att."categoryId"
"""


start_time = time.time()
print('working...')

#gws_df = moist.query(test_q)
gws_df  = gws.gws_q(test_q, 'taxonomy_product_backfeed.value', 4000)

outfile = Path(settings.directory_name)/"test.xlsx"
gws_df.to_excel (outfile, index=None, header=True, encoding='utf-8')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
