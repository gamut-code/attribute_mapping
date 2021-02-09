"""
Search for values using LIKE, edit query for specific values
"""

import pandas as pd
import numpy as np
from postgres_GWS import PostgresDatabase_GWS
import time

gws = PostgresDatabase_GWS()


#def test_query(search, k):
attr_values_1="""
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
				, tprodvalue.value as "Original_Value"
				, tprodvalue.unit as "Original_Unit"
				, tprodvalue."valueNormalized" as "Normalized_Value"
				, tprodvalue."unitNormalized" as "Normalized_Unit"
			, tprodvalue."numeratorNormalized" as "Numerator"
			, tprodvalue."denominatorNormalized" as "Denominator"
				, tax_att."unitGroupId" as "Unit_Group_ID"

		FROM  taxonomy_product tprod

		INNER JOIN tax
				ON tax.id = tprod."categoryId"
				--  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
				AND tprod.status = 3

		INNER JOIN taxonomy_attribute tax_att
				ON tax_att."categoryId" = tprod."categoryId"
				AND tax_att.deleted = 'false'

		INNER JOIN  taxonomy_product_attribute_value tprodvalue
				ON tprod.id = tprodvalue."productId"
				AND tax_att.id = tprodvalue."attributeId"
				AND tprodvalue.deleted = 'false'

		INNER JOIN pi_mappings
				ON pi_mappings.gws_attribute_ids[1] = tax_att.id
				AND pi_mappings.gws_category_id = tax_att."categoryId"

		WHERE tax_att."dataType" IN ('text')
		 AND tprodvalue.value LIKE '%% in%%'
"""

attr_values_2="""
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
				, tprodvalue.value as "Original_Value"
				, tprodvalue.unit as "Original_Unit"
				, tprodvalue."valueNormalized" as "Normalized_Value"
				, tprodvalue."unitNormalized" as "Normalized_Unit"
			, tprodvalue."numeratorNormalized" as "Numerator"
			, tprodvalue."denominatorNormalized" as "Denominator"
				, tax_att."unitGroupId" as "Unit_Group_ID"

		FROM  taxonomy_product tprod

		INNER JOIN tax
				ON tax.id = tprod."categoryId"
				--  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
				AND tprod.status = 3

		INNER JOIN taxonomy_attribute tax_att
				ON tax_att."categoryId" = tprod."categoryId"
				AND tax_att.deleted = 'false'

		INNER JOIN  taxonomy_product_attribute_value tprodvalue
				ON tprod.id = tprodvalue."productId"
				AND tax_att.id = tprodvalue."attributeId"
				AND tprodvalue.deleted = 'false'

		INNER JOIN pi_mappings
				ON pi_mappings.gws_attribute_ids[1] = tax_att.id
				AND pi_mappings.gws_category_id = tax_att."categoryId"

		WHERE tax_att."dataType" IN ('text')
		 AND tprodvalue.value LIKE '%%in %%'
"""


def data_out(ws_df, batch=''):
		outfile = 'C:/Users/xcxg109/NonDriveFiles/ATTR_VALUES_'+str(batch)+'.xlsx'
		ws_df.to_excel (outfile, index=None, header=True, encoding='utf-8')


start_time = time.time()
print('working...')

ws_df = gws.query(attr_values_1)
print('WS length = ', len(ws_df))

ws2_df = gws.query(attr_values_2)
print('WS length = ', len(ws2_df))

ws_df = pd.concat([ws_df, ws2_df], axis=0, sort=False)
ws_df.drop_duplicates()

if len(ws_df) > 900000:
		count = 1
		# split into multiple dfs of 40K rows, creating at least 2
		num_lists = round(len(ws_df)/900000, 0)
		num_lists = int(num_lists)
		if num_lists == 1:
				num_lists = 2
		print('creating {} output files'.format(num_lists))
		# np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
		split_df = np.array_split(ws_df, num_lists)
		for object in split_df:
				print('iteration {} of {}'.format(count, num_lists))
				data_out(object, count)
				count += 1
else:
		data_out(ws_df)


print("--- {} seconds ---".format(round(time.time() - start_time, 2)))