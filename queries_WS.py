# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 12:56:37 2019

@author: xcxg109
"""


gws_basic_query="""
    SELECT
          tprod."gtPartNumber" as "WS_SKU"
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
        
    WHERE {} IN ({})
        """

#get basic SKU list and hierarchy data from Grainger teradata material universe
gws_hier_query="""
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
                array_to_string(tax.ancestor_names || tax.name,' > ') as "GWS_PIM_Path"
                , tax.ancestors[1] as "GWS_Category_ID"
                , tax.ancestor_names[1] as "GWS_Category_Name"
                , tprod."categoryId" AS "GWS_Node_ID"
                , tax.name as "GWS_Node_Name"
                , tprod."gtPartNumber" as "WS_SKU"
                , tprod.id as "PIM_ID"

            FROM taxonomy_product tprod

            INNER JOIN tax
                ON tax.id = tprod."categoryId"
                AND tprod.status = 3
        
            WHERE {} IN ({})
            """

#pull short descriptions from the gamut postgres database
gws_short_query="""
        WITH RECURSIVE merch AS (
                SELECT  id,
			        name,
                    ARRAY[]::INTEGER[] AS ancestors,
                    ARRAY[]::character varying[] AS ancestor_names
                FROM    merchandising_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false
                 and category.visible = true

                UNION ALL

                SELECT  category.id,
			category.name,
                    merch.ancestors || category."parentId",
                    merch.ancestor_names || parent_category.name
                FROM    merchandising_category as category
                    JOIN merch on category."parentId" = merch.id
                    JOIN merchandising_category parent_category on category."parentId" = parent_category.id
                WHERE   category.deleted = false
			and category.visible = true		
            )

           SELECT tprod."gtPartNumber" AS "WS_SKU"
            , mprod.description AS "WS_Product_Description"
            , mprod."merchandisingCategoryId" AS "WS_Merch_Node"
            , mcoll.name as "WS_Collection"
            
            FROM  merchandising_product as mprod   

            INNER JOIN taxonomy_product AS tprod
                ON tprod.id = mprod."taxonomyProductId"
                AND mprod.deleted = 'f'

            INNER JOIN merchandising_collection_product mcollprod
                ON mprod.id = mcollprod."merchandisingProductId"

            INNER JOIN merchandising_collection as mcoll
                ON mcoll.id = mcollprod."collectionId"

            WHERE tprod.deleted = 'f'
                AND {} IN ({})
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


#get basic SKU list and hierarchy data from Grainger teradata material universe
grainger_hier_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.PM_CODE
            , item.SALES_STATUS
            , yellow.PROD_CLASS_ID AS Gcom_Yellow
            , flat.Web_Parent_Name AS Gcom_Web_Parent
            , supplier.SUPPLIER_NO AS Supplier_ID
            , supplier.SUPPLIER_NAME AS Supplier


            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            
            RIGHT JOIN PRD_DWH_VIEW_LMT.ITEM_V AS item
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code

            INNER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                on prod.MATERIAL = item.MATERIAL_NO

            INNER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO
                                
            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
            	AND {} IN ({})
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
            , item.PM_CODE AS PM_Code
            , item.SALES_STATUS as Sales_Status
            , item.RELATIONSHIP_MANAGER_CODE
--            , attr.attribute_level_definition as Grainger_Attribute_Definition
--            , cat_desc.cat_specific_attr_definition as Grainger_Category_Specific_Definition

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
            
grainger_value_query="""
           	SELECT cat.SEGMENT_NAME AS L1
            , cat.FAMILY_NAME AS L2
            , cat.CATEGORY_ID AS L3
            , cat.CATEGORY_NAME
            , item.MATERIAL_NO AS Grainger_SKU
            , item.MFR_MODEL_NO AS Mfr_Part_No
            , attr.DESCRIPTOR_NAME AS Attribute
            , item_attr.ITEM_DESC_VALUE AS Attribute_Value
            , item.PM_CODE AS PM_Code
            , item.SHORT_DESCRIPTION AS Item_Description
            , yellow.PROD_CLASS_ID AS Yellow_ID


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
            
            
STEP_ETL_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SUPPLIER_NO
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS
            , item.PRICING_FLAG
            , item.PRICER_FIRST_EFFECTIVE_DATE
            
            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
         		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')

            LEFT OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                ON prod.MATERIAL = item.MATERIAL_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND {} IN ({})
            """
            
            
grainger_attr_ETL_query="""
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

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

           LEFT  JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                ON prod.MATERIAL = item.MATERIAL_NO
                
            LEFT JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND supplier.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)
                AND {} IN ({})
                """


grainger_attr_ALL_query="""
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

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item_attr.CATEGORY_ID

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID

           LEFT  JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                ON prod.MATERIAL = item.MATERIAL_NO
                
            LEFT JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE {} IN ({})
                """


ETL_nodes_query="""
            SELECT
                cat.SEGMENT_ID AS Segment_ID
                , cat.SEGMENT_NAME AS Segment_Name
                , cat.FAMILY_ID AS Family_ID
                , cat.FAMILY_NAME AS Family_Name
                , cat.CATEGORY_ID AS Category_ID
                , cat.CATEGORY_NAME AS Category_Name

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        		AND item.DELETED_FLAG = 'N'
            
            LEFT OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                ON prod.MATERIAL = item.MATERIAL_NO
                
            LEFT OUTER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO
                AND supplier.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)  -- 7 combo IDs

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND {} IN ({})
            """
            
            
#pull item and SEO descrpitions from the grainger teradata material universe
grainger_short_query="""
            SELECT item.MATERIAL_NO AS WS_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SHORT_DESCRIPTION AS Item_Description
            , item.GIS_SEO_SHORT_DESC_AUTOGEN AS SEO_Description
            , item.PM_CODE
            , brand.BRAND_NAME            
            , yellow.PROD_CLASS_ID AS STEP_Yellow
            , flat.Web_Parent_Name AS Gcom_Web_Parent

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')
                AND item.PM_CODE NOT IN ('R4')

            INNER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            INNER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code

            INNER JOIN PRD_DWH_VIEW_MTRL.BRAND_V AS brand
                ON item.BRAND_NO = brand.BRAND_NO

            INNER JOIN PRD_DWH_VIEW_MTRL.SUPPLIER_V AS supplier
            	ON supplier.SUPPLIER_NO = item.SUPPLIER_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
            	AND {} IN ({})
            """


#pull item and SEO descrpitions from the grainger teradata material universe
grainger_short_values="""
            SELECT item.MATERIAL_NO AS WS_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME as Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SHORT_DESCRIPTION AS Item_Description
            , item.GIS_SEO_SHORT_DESC_AUTOGEN AS SEO_Description
            , item.PM_CODE
            , yellow.PROD_CLASS_ID AS STEP_Yellow
            , flat.Web_Parent_Name AS Gcom_Web_Parent

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')
                AND item.PM_CODE NOT IN ('R4')

            INNER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            INNER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code

            INNER JOIN PRD_DWH_VIEW_MTRL.BRAND_V AS brand
                ON item.BRAND_NO = brand.BRAND_NO

            INNER JOIN PRD_DWH_VIEW_MTRL.SUPPLIER_V AS supplier
            	ON supplier.SUPPLIER_NO = item.SUPPLIER_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
            	AND LOWER({}) LIKE LOWER ({})
            """


#variation of the basic query designed to include discontinued items
grainger_discontinued_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.PM_CODE
            , item.SALES_STATUS
            , item.RELATIONSHIP_MANAGER_CODE
            , item.SUPPLIER_NO
            , yellow.PROD_CLASS_ID AS Gcom_Yellow
            , flat.Web_Parent_Name AS Gcom_Web_Parent

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        	--	AND item.DELETED_FLAG = 'N'
                
            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code

            WHERE {} IN ({})
            """


# pull only discontinued SKUs
grainger_ONLY_discontinueds="""
            SELECT cat.SEGMENT_ID AS L1
            , cat.SEGMENT_NAME
            , cat.FAMILY_ID AS L2
            , cat.FAMILY_NAME
            , cat.CATEGORY_ID AS L3
            , cat.CATEGORY_NAME
            , item.MATERIAL_NO AS Grainger_SKU
            , item.MFR_MODEL_NO AS Mfr_Part_No
            , attr.DESCRIPTOR_NAME AS Grainger_Attribute_Name
            , item_attr.ITEM_DESC_VALUE AS Attribute_Value
--            , cat_desc.ENDECA_RANKING
            , item.RELATIONSHIP_MANAGER_CODE AS RMC
            , item.SALES_STATUS AS Sales_Status
            , item.PM_CODE AS PM_Code
--            , yellow.PROD_CLASS_ID AS Yellow_ID

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
                AND attr.DELETED_FLAG = 'N'

  --          INNER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
  --              ON yellow.PRODUCT_ID = item.MATERIAL_NO

            WHERE item.SALES_STATUS IN ('DG', 'DV', 'WV', 'WG')
                AND {} IN ({})
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

    INNER JOIN unit_group
        ON unit_group.id = tax_att."unitGroupId"
        
    FULL OUTER JOIN uom_kind
        ON uom_kind.id = unit_group."kindId"
        
    WHERE {} IN ({})
        """
