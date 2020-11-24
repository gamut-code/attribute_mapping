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
        , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "Attribute_Value_ID"
        , tax_att.description as "Attribute_Definition"
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
        AND tax_att.delete = 'false'

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
--        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , tax_att.id as "WS_Attr_ID"
--        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "WS_Attr_Value_ID"
        , tax_att.description as "Attribute_Definition"
--        , tax_att.description as "WS_Attribute_Definition"
--        , tprodvalue.value as "Original_Value"
--        , tprodvalue.unit as "Original_Unit"
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

def gws_values(df):
    df['WS_Value'] = ''
    
    for row in df.itertuples():
        val = df.at[row.Index, 'Normalized_Value']
        unit = df.at[row.Index, 'Normalized_Unit']
            
        ws_val = str(val) + ' ' + str(unit)
        df.at[row.Index, 'WS_Value'] = ws_val
                    
    return df


def data_out(final_df, node):
    final_df = final_df.sort_values(['WS_Category_Name', 'WS_Node_Name', 'WS_Attribute_Name'], ascending=[True,True,True])
    
    final_no_dupes = final_df.drop_duplicates(subset=['WS_Node_ID', 'WS_Attr_ID', 'Normalized_Unit'])
    final_no_dupes = final_no_dupes [['WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', \
                                'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', 'Attribute_Definition', \
                                'Numeric_Display_Type', 'Unit_Group_ID', 'Normalized_Unit', 'Attribute_Values', \
                                'UOMs in Attribute']]
    final_no_dupes = final_no_dupes.rename(columns={'WS_SKU':'Example SKU'})

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_multi-UOMs.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    final_no_dupes.to_excel (writer, sheet_name="UOMs", startrow=0, startcol=0, index=False)

    worksheet1 = writer.sheets['UOMs']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    col_widths = fd.get_col_widths(final_no_dupes)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    writer.save()
    
        
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

start_time = time.time()
print('working...')

for node in search_data:
    ws_df = pd.DataFrame()

    print('k = ', node)
    
    if search_level == 'single':
        df = gws.gws_q(gws_single, 'tprod."categoryId"', node)
        
    elif search_level == 'group':
        df = gws.gws_q(gws_group, 'tprod."categoryId"', node)
        
    print('k = ', node)
    df['UOMs in Attribute'] = ''
    df['Attribute_Values'] = ''
    
    if df.empty == False:
        atts = df['WS_Attr_ID'].unique()

    attr_ids = df['WS_Attr_ID'].unique()
    
    for attribute in attr_ids:
        temp_df = df.loc[df['WS_Attr_ID']== attribute]
        
        temp_df = temp_df.sort_values(['Normalized_Unit', 'Normalized_Value'], ascending=[True, True])

        uom_list = temp_df['Normalized_Unit'].unique().tolist()

        if None in uom_list:
            uom_list.remove(None)
            
        if len(uom_list) > 1:
            temp_df = gws_values(temp_df)

            for u in uom_list:
                temp_uom_df = temp_df.loc[temp_df['Normalized_Unit']== u]

                attr_values = temp_uom_df['WS_Value'].unique().tolist()

                for row in temp_uom_df.itertuples():
                    temp_uom_df.at[row.Index, 'UOMs in Attribute'] = uom_list
                    temp_uom_df.at[row.Index, 'Attribute_Values'] = attr_values

                ws_df = pd.concat([ws_df, temp_uom_df], axis=0, sort=False) 

    if ws_df.empty == False:
        data_out(ws_df, node)
    else:
        print ('no multi UOM values for node ', node)

    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
