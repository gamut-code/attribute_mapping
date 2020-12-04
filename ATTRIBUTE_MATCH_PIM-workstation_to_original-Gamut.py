# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 12:53:25 2020

Inputs: upload of Gamut and WS attribute data files (queries to be run externally, but included here for reference)

Function: comparison of Nodes and Attributes between WS and Gamut, matching based on attribute naming conventions

@author: xcxg109
"""

import pandas as pd
import re
import time
import math
import file_data_GWS as fd
import WS_query_code as q


# query to get WS attributes & values
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
        , tax_att.description as "WS_Attribute_Definition"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "WS_Original_Value"
        , tprodvalue.unit as "WS_Original_Unit"
        , tprodvalue."valueNormalized" as "WS_Normalized_Value"
        , tprodvalue."unitNormalized" as "WS_Normalized_Unit"
   
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

# gamut version of the attribute/values query
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
        , tax_att."multiValue" as "Multivalue"
        , tax_att."dataType" as "Data_Type"
  	    , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tax_att.description as "Gamut_Attribute_Definition"
        , tax_att.name as "Gamut_Attribute_Name"
        , tprodvalue.value as "Gamut_Original Value"
        , tprodvalue.unit as "Gamut_Original_Unit"
        , tprodvalue."valueNormalized" as "Gamut_Normalized Value"
        , tprodvalue."unitNormalized" as "Gamut_Normalized_Unit"

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


def ws_values(df):
    """find the top 10 most used values for each attribute and return as sample_values"""
    all_vals = pd.DataFrame()
    comma_list = list()
    
    func_df = df.copy()
    func_df['Count'] =1
    func_df['Comma Separated Values'] = ''
    print('func_df = ', func_df.columns)
    atts = func_df['WS_Attribute_Name'].unique().tolist()
    
    # remove Item and Series from attribute counts (** specific terms)
    i = 'Item' in atts
    s = 'Series' in atts

    if i: 
        atts.remove('Item')
    if s: 
        atts.remove('Series')

    # remove 'Green' attributes based on general pattern match
    atts = [ x for x in atts if 'Green Certification' not in x ]
    atts = [ x for x in atts if 'Green Environmental' not in x ]

    vals = pd.DataFrame(func_df.groupby(['WS_Attr_ID', 'WS_Attribute_Name', 'WS_Attribute_Value'])['Count'].sum())
    vals = vals.reset_index()
 
    for attribute in atts:
        temp_df = vals.loc[vals['WS_Attribute_Name']== attribute]
        temp_df = temp_df.sort_values(by=['Count'], ascending=[False])

        # build a list of comma separated attributes to help determine if a multi value is needed        
        subs = ','
        comma_list = temp_df['WS_Attribute_Value'].to_list()
        comma_list = [i for i in comma_list if subs in i] 
        
        regex = re.compile(r'\d+,\d+')
        exclude_list = list(filter(regex.match, comma_list))
                
        set_difference = set(comma_list) - set(exclude_list)
        diff = list(set_difference)
        diff = '; '.join(diff)

        temp_df['Comma Separated Values'] = diff
        
        # concat list items into string
        temp_df['WS ALL Values'] = '; '.join(item for item in temp_df['WS_Attribute_Value'] if item)
        
        #pull the top 10 values and put into 'Sample_Values' field
        temp_att = temp_df.head(10)
        temp_df['Sample_Values'] = '; '.join(item for item in temp_att['WS_Attribute_Value'] if len(item)<250)
        all_vals = pd.concat([all_vals, temp_df], axis=0)

    if all_vals.empty == False:
        all_vals = all_vals[['WS_Attr_ID', 'WS ALL Values', 'Comma Separated Values', 'Sample_Values']]
        all_vals = all_vals.drop_duplicates(subset=['WS_Attr_ID'])

    return all_vals













# read in WS data
print('Choose WS PIM file')
allCATS_df = q.get_att_values()

# read in and clean WS data
print('\nChoose Gamut file')
Gamut_allCATS_df = q.get_att_values()

print('working...')
start_time = time.time()

node_ids = allCATS_df['Category_ID'].unique().tolist()
print('number of nodes = ', len(node_ids))


for node in node_ids:
    temp_df = allCATS_df.loc[allCATS_df['Category_ID']== node]
    
    if temp_df == False:
        grainger_att_vals, grainger_sample_vals = q.grainger_values(grainger_df)
