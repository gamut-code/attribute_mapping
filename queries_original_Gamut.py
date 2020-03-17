# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 12:56:37 2019

@author: xcxg109
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

gamut_usage_query="""
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
            
, merch AS (
    SELECT  id,
    		name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
            
    FROM    merchandising_category as category
    
    WHERE   "parentId" IS NULL
        AND category.deleted = false
        AND category.visible = true

    UNION ALL

    SELECT  category.id,
			category.name,
            merch.ancestors || category."parentId",
            merch.ancestor_names || parent_category.name
            
    FROM    merchandising_category as category
    
    JOIN merch on category."parentId" = merch.id
    JOIN merchandising_category parent_category on category."parentId" = parent_category.id
    
    WHERE   category.deleted = false
        AND category.visible = true		
    )

, _tmp_attrs AS (
/*			--stack pivots*/
	SELECT
		collord.node_id
		, collord.collection_id
		, collord.collection_name
		, collord.collection_visible
		, 'stack' as attr_type
		, collord.attribute_id
        
	FROM	( 
        SELECT
			coll."merchandisingCategoryId" as node_id
			, coll.id as collection_id
			, coll.name as collection_name
			, coll.visible as collection_visible
			, json_extract_path_text(json_array_elements(json_extract_path(json_array_elements("stacksConfiguration"), 'groupSelections')),'merchandisingAttributeId')::integer as attribute_id	
            , row_number() over() as coll_order

        FROM merchandising_collection coll
        
        WHERE coll.deleted = 'f'	
            AND coll.visible = 't'
	) collord	

	UNION
/*	--Table attributes*/
	SELECT
		collord.node_id
		, collord.collection_id
		, collord.collection_name
		, collord.collection_visible
		, 'table' as attr_type
		, collord.attribute_id
        
	FROM	( 
        SELECT
			coll."merchandisingCategoryId" as node_id
			, coll.id as collection_id
			, coll.name as collection_name
			, coll.visible as collection_visible
			, json_extract_path_text(json_array_elements("orderedVisibleMerchandisingAttributes"), 'merchandisingAttributeId')::integer  as attribute_id
            , row_number() over() as coll_order
            
        FROM merchandising_collection coll
        
        WHERE coll.deleted = 'f' 	
            AND coll.visible = 't'
    ) collord

	UNION
/*	--Column groups*/
	SELECT
		collord.node_id
		, collord.collection_id
		, collord.collection_name
		, collord.collection_visible
		, 'column group' as attr_type
		, collord.attribute_id
        
	FROM	( 
        SELECT
			coll."merchandisingCategoryId" as node_id
			, coll.id as collection_id
			, coll.name as collection_name
			, coll.visible as collection_visible
			, (json_extract_path(json_array_elements("columnGroupsConfiguration"), 'merchandisingAttributeId')::text)::integer as attribute_id	
            , row_number() over() as coll_order
            
        FROM merchandising_collection coll
        
        WHERE coll.deleted = 'f'	
            AND coll.visible = 't'
    ) collord

	UNION
/*	-- filters*/
	SELECT 
		z.node_id
		, coll.id  as collection_id
		, coll.name as collection_name
		, coll.visible as collection_visible
		, attr_type
		, attribute_id
        
	FROM (
		SELECT
			 node_id
			, 'filter'::text as attr_type
			, attribute_id
            
		FROM	(
            SELECT
                cat.id as node_id
				, unnest("sortedFiltersKeys")::integer  as attribute_id
				, "sortedFiltersKeys"
                
            FROM merchandising_category cat	
            
            WHERE cat.deleted = 'f'
                AND cat.visible = 't'
			) y		   
		)z 
    
	LEFT JOIN merchandising_collection coll
		ON coll."merchandisingCategoryId" = z.node_id
        AND coll.visible = 't'
    	)
            

    SELECT DISTINCT ON (_tmp_attrs.attr_type, tax_att.id)
         tax_att.id as "Gamut_Attr_ID"
        , _tmp_attrs.attr_type as "Gamut_MERCH_Usage"
		
FROM  merchandising_product as mprod

INNER JOIN merch
  ON merch.id = mprod."merchandisingCategoryId"
  
INNER JOIN taxonomy_product as tprod
  ON tprod.id = mprod."taxonomyProductId"
	AND mprod.deleted = 'f'

INNER JOIN merchandising_collection_product mcollprod
  ON mprod.id = mcollprod."merchandisingProductId"

INNER JOIN merchandising_collection as mcoll
  ON mcoll.id = mcollprod."collectionId"

INNER JOIN _tmp_attrs
	ON _tmp_attrs.collection_id = mcollprod."collectionId"
	  
INNER JOIN  merchandising_product_value mprodvalue
    ON mprodvalue."merchandisingProductId" = mcollprod."merchandisingProductId"
    AND mprodvalue."merchandisingAttributeId" = _tmp_attrs.attribute_id
    AND mprodvalue.deleted = 'f'

INNER JOIN merchandising_attribute merchatt
    ON merchatt.id = _tmp_attrs.attribute_id
    AND merchatt.deleted = 'f'

INNER JOIN merchandising_attribute__taxonomy_attribute merchAtt_taxAtt
    ON merchAtt_taxAtt."merchandisingAttributeId" = merchatt.id

INNER JOIN taxonomy_attribute tax_att
    ON tax_att.id = merchAtt_taxAtt."attributeId"

INNER JOIN tax
    ON tax.id = tax_att."categoryId"

WHERE tprod.deleted = 'f'
  AND mcoll.visible = 't'
  AND {} IN ({})
    """
                        
#pull attribute values from Grainger teradata material universe by L3
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