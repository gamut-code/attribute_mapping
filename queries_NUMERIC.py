# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 12:56:37 2019

@author: xcxg109
"""


gws_basic_query="""
    SELECT
          tprod."gtPartNumber" as "GWS_SKU"
        , tprod."gtPartNumber" as "Grainger_SKU"
        , tprod."categoryId" AS "GWS_Node_ID"
        
    FROM taxonomy_product tprod
    
    WHERE {} IN ({})
"""

gws_attr_query="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "GWS_PIM_Path"
        , tax.ancestors[1] as "GWS_Category_ID"  
        , tax.ancestor_names[1] as "GWS_Category_Name"
        , tax_att."categoryId" AS "GWS_Node_ID"
        , tax.name as "GWS_Node_Name"
        , tax_att.id as "GWS_Attr_ID"
        , tax_att.name as "GWS_Attribute_Name"
        , tax_att.description as "GWS_Attribute_Definition"
   
    FROM  taxonomy_attribute tax_att

    INNER JOIN tax
        ON tax.id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """

gws_attr_values="""
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
        , tprod."gtPartNumber" as "Grainger_SKU"
        , tax_att.id as "Gamut_Attr_ID"
        , tax_att.name as "Gamut_Attribute_Name"
        , tax_att.description as "Gamut_Attribute_Definition"
        , tprodvalue.value as "Original Value"
        , tprodvalue."valueNormalized" as "Normalized Value"
        , tax_att."unitGroupId" as "Unit_Group_ID"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE {} IN ({})
        """
            
#pull attribute values from Grainger teradata material universe by L3
grainger_attr_query="""
           	SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.MATERIAL_NO AS Grainger_SKU
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID
            , attr.DESCRIPTOR_NAME as Grainger_Attribute_Name
            , item_attr.ITEM_DESC_VALUE as Grainger_Attribute_Value
--            , item.PM_CODE AS PM_Code
--            , item.SALES_STATUS as Sales_Status
            , attr.attribute_level_definition as Grainger_Attribute_Definition
            , cat_desc.cat_specific_attr_definition as Grainger_Category_Specific_Definition

            FROM PRD_DWH_VIEW_MTRL.ITEM_DESC_V AS item_attr

            INNER JOIN PRD_DWH_VIEW_MTRL.ITEM_V AS item
                ON 	item_attr.MATERIAL_NO = item.MATERIAL_NO
                AND item.DELETED_FLAG = 'N'
                AND item_attr.DELETED_FLAG = 'N'
                AND item_attr.LANG = 'EN'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item_attr.CATEGORY_ID
                AND item_attr.DELETED_FLAG = 'N'

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE {} IN ({})
            """
            

#get basic SKU list and hierarchy data from Grainger teradata material universe
grainger_basic_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.PM_CODE
            , item.SALES_STATUS


            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            
            RIGHT JOIN PRD_DWH_VIEW_LMT.ITEM_V AS item
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
                                
            WHERE {} IN ({})
            """
            
gamut_basic_query="""
    SELECT
          tprod."gtPartNumber" as "Gamut_SKU"
        , tprod."supplierSku" as "Grainger_SKU"
        , tprod."categoryId" AS "Gamut_Node_ID"
        
    FROM taxonomy_product tprod
    
    WHERE {} IN ({})
"""
