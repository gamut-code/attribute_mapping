# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import requests
import string
import re
from collections import defaultdict
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_WS import gws_attr_query, gws_attr_values, grainger_attr_ETL_query, grainger_attr_ALL_query
import data_process as process
import WS_query_code as q
import file_data_GWS as fd
from typing import Dict
import settings_NUMERIC as settings
import time
import memory_clear as mem

pd.options.mode.chained_assignment = None

gcom = GraingerQuery()
gws = GWSQuery()



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
        , tax_att.id as "WS_Attr_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "Attribute_Value_ID"
--        , tax_att.description as "Gamut_Attribute_Definition"
        , tprodvalue.value as "WS_Original Value"
        , tprodvalue."valueNormalized" as "WS_Normalized Value" 
        , tprodvalue."unitNormalized" as "WS_Unit_Normalized"
        , back."stepAttributeValue" as "STEP_Backfeed_Value"        
        , tprodvalue."baseValue" as "Base_Value?"
        , tprodvalue."inputValue" as "Input_Value?"
        , tprodvalue."inputValueNormalized" as "Input_Value_Normalized?"
        , tax_att."unitGroupId" as "Unit_Group_ID"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"

    INNER JOIN taxonomy_product_value_backfeed back
        ON back."taxonomyProductAttributeValueId" = tprodvalue.id
    
    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
    
    WHERE tax_att."unitGroupId" IN (266)
        """

    
def process_sample_vals(df, row, pot):
    """ clean up the sample values column """

    potential_list = list(pot.split(', '))
    
    sample_val = str(row.Sample_Values)
    LOV_val = str(row.Restricted_Attribute_Value_Domain)
    
    for uom in potential_list:
        if '"' in str(uom):
            sample_val = sample_val.replace('"', ' in')
            LOV_val = LOV_val.replace('"', ' in')

        if 'in.' in str(uom):
            sample_val = sample_val.replace('in.', 'in')
            LOV_val = LOV_val.replace('in.', 'in')

        if 'ft.' in str(uom):
            sample_val = sample_val.replace('ft.', 'ft')
            LOV_val = LOV_val.replace('ft.', 'ft')
            
        if 'yd.' in str(uom):
            sample_val = sample_val.replace('yd.', 'yd')   
            LOV_val = LOV_val.replace('yd.', 'yd')   
        
        if 'fl.' in str(uom):
            sample_val = sample_val.replace('fl.', 'fl')    
            LOV_val = LOV_val.replace('fl.', 'fl')    
        
        if 'oz.' in str(uom):
            sample_val = sample_val.replace('oz.', 'oz')    
            LOV_val = LOV_val.replace('oz.', 'oz')    
        
        if 'pt.' in str(uom):
            sample_val = sample_val.replace('pt.', 'pt')    
            LOV_val = LOV_val.replace('pt.', 'pt')    

        if 'qt.' in str(uom):
            sample_val = sample_val.replace('qt.', 'qt')     
            LOV_val = LOV_val.replace('qt.', 'qt')     

        if 'kg.' in str(uom):
            sample_val = sample_val.replace('kg.', 'kg')    
            LOV_val = LOV_val.replace('kg.', 'kg')    
        
        if 'gal.' in str(uom):
            sample_val = sample_val.replace('gal.', 'gal') 
            LOV_val = LOV_val.replace('gal.', 'gal') 
        
        if 'lb.' in str(uom):
            sample_val = sample_val.replace('lb.', 'lb')   
            LOV_val = LOV_val.replace('lb.', 'lb')   
        
        if 'cu.' in str(uom):
            sample_val = sample_val.replace('cu.', 'cu')  
            LOV_val = LOV_val.replace('cu.', 'cu')  
        
        if 'sq.' in str(uom):
            sample_val = sample_val.replace('sq.', 'sq')    
            LOV_val = LOV_val.replace('sq.', 'sq')    

        if '° C' in str(uom):
            sample_val = sample_val.replace('° C', '°C')
            LOV_val = LOV_val.replace('° C', '°C')

        if '° F' in str(uom):
            sample_val = sample_val.replace('° F', '°F')     
            LOV_val = LOV_val.replace('° F', '°F')     
        
        if 'deg.' in str(uom):        
            sample_val = sample_val.replace('deg.', '°')        
            LOV_val = LOV_val.replace('deg.', '°')        

        if 'ga.' in str(uom):        
            sample_val = sample_val.replace('ga.', 'ga')        
            LOV_val = LOV_val.replace('ga.', 'ga')        

        if 'point' in str(uom):
            sample_val = sample_val.replace('point', 'pt.')        
            LOV_val = LOV_val.replace('point', 'pt.')        

        if 'min.' in str(uom):
            sample_val = sample_val.replace('min.', 'min')
            LOV_val = LOV_val.replace('min.', 'min')

        if 'sec.' in str(uom):
            sample_val = sample_val.replace('sec.', 'sec')
            LOV_val = LOV_val.replace('sec.', 'sec')

        if 'hr.' in str(uom):
            sample_val = sample_val.replace('hr.', 'hr')        
            LOV_val = LOV_val.replace('hr.', 'hr')        

        if 'wk.' in str(uom):
            sample_val = sample_val.replace('wk.', 'wk') 
            LOV_val = LOV_val.replace('wk.', 'wk') 

        if 'mo.' in str(uom):
            sample_val = sample_val.replace('mo.', 'mo')
            LOV_val = LOV_val.replace('mo.', 'mo')

        if 'yr.' in str(uom):
            sample_val = sample_val.replace('yr.', 'yr')
            LOV_val = LOV_val.replace('yr.', 'yr')

        if 'µ' in str(uom):
            sample_val = sample_val.replace('µ', 'u')        
            LOV_val = LOV_val.replace('µ', 'u')        

    df.at[row.Index,'Sample_Values'] = sample_val
    df.at[row.Index,'Restricted_Attribute_Value_Domain'] = LOV_val

    return df 


df_upload = pd.DataFrame()
search_level = 'cat.CATEGORY_ID'
quer = 'ATTR'

start_time = time.time()
print('working...')

gws_df = gws.gws_q(gws_attr_values, 'tax_att."unitGroupId"', 266)
fd.data_out(settings.directory_name, gws_df, quer, search_level)


print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))