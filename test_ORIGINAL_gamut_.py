 # -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#ORIGINAL Gamut Test
#from postgres_client import PostgresDatabase
#db = PostgresDatabase()

#1.5 ADMIN Gamut Test
from postgres_ORIGINAL_gamut import PostgresDatabase
from pathlib import Path
import pandas as pd
import numpy as np
import settings
from typing import Dict
import query_code_original_Gamut as q
from queries_original_Gamut import gamut_attr_query, gamut_usage_query

moist = PostgresDatabase()



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
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
	AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tprod.deleted = 'f'
           AND {term} IN ({k})
""".format(term='tprod."categoryId"', k=1951)

def gamut_process(node):
    """if gamut node has not been previously processed (in gamut_dict), process and add it to the dictionary"""
#    gamut_sample_vals = pd.DataFrame()
#    gamut_att_vals = pd.DataFrame()
    gamut_df = pd.DataFrame()

    gamut_df_1 = q.gamut_atts(gamut_attr_query, node, 'tax_att."categoryId"')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\
    gamut_df_1.to_csv("F:\CGabriel\Grainger_Shorties\OUTPUT\gamut_DF1.csv")
    if gamut_df_1.empty==False:
        gamut_df_2 = q.gamut_atts(gamut_usage_query, node, 'tax_att."categoryId"')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\
        
        if gamut_df_2.empty==False:
            gamut_df_2 = gamut_df_2.groupby(['Gamut_Attr_ID'])['Gamut_MERCH_Usage'].apply('; '.join).reset_index()
            gamut_df_2.to_csv("F:\CGabriel\Grainger_Shorties\OUTPUT\gamut_DF2.csv")

#            gamut_df = pd.concat([gamut_df_1, gamut_df_2], axis=0, join='outer', sort=False)
            gamut_df = pd.merge(gamut_df_1, gamut_df_2, how = 'outer', on = 'Gamut_Attr_ID')
        else:
            gamut_df = gamut_df_1
            gamut_df['Gamut_MERCH_Usage'] = ""
            
        gamut_df.loc[gamut_df['Gamut_MERCH_Usage'] == '', 'Gamut_MERCH_Usage'] = np.nan
        gamut_df = gamut_df.sort_values(['Gamut_Attr_ID', 'Gamut_MERCH_Usage']).drop_duplicates(subset = 'Gamut_Attr_ID')
        gamut_df.to_csv("F:\CGabriel\Grainger_Shorties\OUTPUT\gamut_combined.csv")

    return gamut_df


#gamut_df = moist.query(test_q)

gamut_df = gamut_process(5934)

outfile = Path(settings.directory_name)/"2_test.xlsx"
gamut_df.to_excel(outfile)
