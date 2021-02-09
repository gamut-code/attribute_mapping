# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 14:53:38 2021

@author: xcxg109
"""
step_cat_name="""

            , cat.CATEGORY_ID AS Category_ID
          , cat.CATEGORY_NAME AS Category_Name
          
                      FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
"""

ws_cat_name="""

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
                              
                              catID
                              node name
"""


pull step and gws node names and compare them
    do they match?
        false?
            compare first __ number of characters - as a second check
                              
                            