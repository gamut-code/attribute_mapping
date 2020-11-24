# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 13:15:15 2020

@author: xcxg109
"""

import pandas as pd
from GWS_query import GWSQuery
import file_data_GWS as fd
import settings_NUMERIC as settings
import time

gws = GWSQuery()

gws_single="""
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
        , tax_att.id as "WS_Attr_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "Attribute_Value_ID"
--        , tax_att.description as "WS_Attribute_Definition"
        , tprodvalue.value as "Original_Value"
        , tprodvalue.unit as "Original_Unit"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        , tprodvalue."unitNormalized" as "Normalized_Unit"
        , tax_att."unitGroupId" as "Unit_Group_ID"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att."dataType" = 'number'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """
        

gws_group="""
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
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , tax_att.id as "WS_Attr_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "Attribute_Value_ID"
--        , tax_att.description as "WS_Attribute_Definition"
        , tprodvalue.value as "Original_Value"
        , tprodvalue.unit as "Original_Unit"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        , tprodvalue."unitNormalized" as "Normalized_Unit"
        , tax_att."unitGroupId" as "Unit_Group_ID"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att."dataType" = 'number'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"        
        """
        
        
ws_df = pd.DataFrame()
data_type = 'gws_query'

while True:
    try:
        search_level = input("Search by: \n1. Node Group \n2. Single Category ")
        if search_level in ['1', 'g', 'G']:
            search_level = 'group'
            break
        elif search_level in ['2', 's', 'S']:
            search_level = 'single'
            break
    except ValueError:
        print('Invalid search type')
        
search_data = fd.data_in(data_type, settings.directory_name)

print('working...')

for node in search_data:
    start_time = time.time()

    if search_level == 'single':
        df = gws.gws_q(gws_single, 'tprod."categoryId"', node)
        
    elif search_level == 'group':
        df = gws.gws_q(gws_group, 'tprod."categoryId"', node)
        
    print('k = ', node)
    
    if df.empty == False:
        atts = df['WS_Attr_ID'].unique()
