# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 12:56:37 2019

@author: xcxg109
"""

            
gamut_basic_query="""
    SELECT
          tprod."gtPartNumber" as "Gamut_SKU"
        , tprod."supplierSku" as "Grainger_SKU"
        , tprod."categoryId" AS "Gamut_Node_ID"
        
    FROM taxonomy_product tprod
    
    WHERE {} IN ({})
"""

gamut_attr_query="""
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
        , tax_att."categoryId" AS "Gamut_Node_ID"
        , tax.name as "Gamut_Node_Name"
        , tax_att.id as "Gamut_Attr_ID"
        , tax_att.name as "Gamut_Attribute_Name"
        , tax_att.description as "Gamut_Attribute_Definition"
   
    FROM  taxonomy_attribute tax_att

    INNER JOIN tax
        ON tax.id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """

gamut_attr_values="""
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
        
    WHERE {} IN ({})
        """


#get basic SKU list and hierarchy data from Grainger teradata material universe
gamut_hier_query="""
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
                tprod."gtPartNumber" as "Gamut_SKU"
                , tprod."supplierSku" as "Grainger_SKU"
                , array_to_string(tax.ancestor_names || tax.name,' > ') as "tax_path"
                , tprod."categoryId" as "PIM Node ID"

            FROM taxonomy_product tprod

            INNER JOIN tax
                ON tax.id = tprod."categoryId"
                -- AND (10006 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

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
                AND item.PM_CODE NOT IN ('R9')

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND attr.DELETED_FLAG = 'N'

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
                AND {} IN ({})
            """
            
grainger_name_query="""
           	SELECT cat.SEGMENT_NAME AS Segment_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_NAME AS Category_Name
            , cat.CATEGORY_ID AS Category_ID
            , item.MATERIAL_NO AS Grainger_SKU
            , attr.DESCRIPTOR_NAME AS Grainger_Attribute_Name
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID

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
                AND item.PM_CODE NOT IN ('R9')

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND attr.DELETED_FLAG = 'N'

            INNER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
                AND LOWER({}) LIKE LOWER({})
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

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            
            RIGHT JOIN PRD_DWH_VIEW_LMT.ITEM_V AS item
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')
                                
            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
            	AND {} IN ({})
            """