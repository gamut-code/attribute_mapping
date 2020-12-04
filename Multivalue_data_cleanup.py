# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 15:24:21 2020

@author: xcxg109

This file looks at all multivalues in the PIM workstation, pulls the STEP equivalent, and then searches for 
meet the conditions for review found in the process_vals module.
"""

import pandas as pd
import numpy as np
import WS_query_code as q
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd

import settings_NUMERIC as settings
import re
import time

pd.options.mode.chained_assignment = None

gws = GWSQuery()
gcom = GraingerQuery()


grainger_vals="""
           	SELECT item.MATERIAL_NO AS Grainger_SKU
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID
            , item_attr.ITEM_DESC_VALUE AS Grainger_Attribute_Value

            FROM PRD_DWH_VIEW_MTRL.ITEM_DESC_V AS item_attr

            INNER JOIN PRD_DWH_VIEW_MTRL.ITEM_V AS item
                ON 	item_attr.MATERIAL_NO = item.MATERIAL_NO
                AND item.DELETED_FLAG = 'N'
                AND item_attr.DELETED_FLAG = 'N'
                AND item_attr.LANG = 'EN'
 
            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE {} IN ({})
            """


gws_attr_single="""
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
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att.deleted = 'false'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        AND tprodvalue.deleted = 'false'
        AND tax_att."multiValue" = 'true'

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """
        
gws_attr_group="""
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
        , {} AS "WS_Node_ID"               --  cheat to use tprod."categoryId" to make the group search work
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
        AND ({} = ANY(tax.ancestors)) -- *** ADD TOP LEVEL NODES HERE ***
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att.deleted = 'false'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        AND tprodvalue.deleted = 'false'
        AND tax_att."multiValue" = 'true'

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"        
        """
        
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        
def process_vals(df):
    df['Potential_Issue'] = ''
    
    for row in df.itertuples():
        orig_value = df.at[row.Index, 'Normalized_Value']
        orig_value = str(orig_value)
        
        lower_val = orig_value.lower()
        
        search_string = ''
    
        if orig_value.startswith('and'):
            search_string = search_string + '; ' + 'starts with and'
    
    #    if re.search('mfr' in orig_value, re.IGNORECASE):
        if 'mfr' in lower_val:
            search_string = search_string + '; ' + 'contains Mfr.'
            
        if ',' in orig_value:
            search_string = search_string + '; ' + 'contains comma'
    
        if 'includes' in lower_val:
            search_string = search_string + '; ' + 'contains includes'
            
        if 'with' in lower_val:
            search_string = search_string + '; ' + 'contains with'
            
        # search for values that start with a lowercase char
        re_pattern = '^[a-z]'
        if re.match(re_pattern, orig_value):
            search_string = search_string + '; ' + 'starts with lowercase chararacter'
    
        # search for values with the pattern "each" and numbers
        re_pattern = '[1-9]+'
        if 'each' in lower_val and re.findall(re_pattern, orig_value):
            search_string = search_string + '; ' + 'each w/number'                
    
        search_string = search_string[2:]
        df.at[row.Index,'Potential_Issue'] = search_string

    return df


def get_STEP_vals(gr_df):
    gr_sku_list = pd.DataFrame()

    gr_df['STEP_Attr_ID'] = gr_df['STEP_Attr_ID'].str.replace('_ATTR', '')
    gr_df['STEP_Attr_ID'] = gr_df['STEP_Attr_ID'].str.replace('_GATTR', '')
    gr_df['STEP_Attr_ID'] = gr_df['STEP_Attr_ID'].str.strip()
    gr_df.dropna(subset=['STEP_Attr_ID'], inplace=True)
    gr_df['STEP_Attr_ID'] = gr_df['STEP_Attr_ID'].astype(int)
    
    sku_list = gr_df['WS_SKU'].unique().tolist()

    if len(sku_list)>4000:
        num_lists = round(len(sku_list)/4000, 0)
        num_lists = int(num_lists)

        if num_lists == 1:
            num_lists = 2

        print('running Grainger SKUs in {} batches'.format(num_lists))

        size = round(len(sku_list)/num_lists, 0)
        size = int(size)

        div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]
        
        for k  in range(0, len(div_lists)):
            print('batch {} of {}'.format(k+1, num_lists))
            gr_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
            temp_gr = gcom.grainger_q(grainger_vals, 'item.MATERIAL_NO', gr_skus)
            
            gr_sku_list = pd.concat([gr_sku_list, temp_gr], axis=0, sort=False) 

    else:
        gr_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
        gr_sku_list = gcom.grainger_q(grainger_vals, 'item.MATERIAL_NO', gr_skus)

    gr_df = gr_df.merge(gr_sku_list, how="left", left_on=['WS_SKU', 'STEP_Attr_ID'], \
                                                 right_on=['Grainger_SKU', 'Grainger_Attr_ID'])                            
    return gr_df
    

def data_out(final_df, node, node_name):
    # NOTE: nonconforming rows of df were dropped and df was sorted before being passed here
    
    final_df['concat'] = final_df['WS_Attribute_Name'].map(str) + final_df['Normalized_Value'].map(str)
    final_df['Group_ID'] = final_df.groupby(final_df['concat']).grouper.group_info[0] + 1

    final_df = final_df[['Group_ID', 'PIM_Path', 'WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', \
                   'STEP_Category_ID', 'WS_SKU', 'STEP_Attr_ID', 'WS_Attr_ID', 'WS_Attr_Value_ID', 'Multivalue', \
                   'Data_Type', 'Numeric_Display_Type', 'WS_Attribute_Name', 'Original_Value', 'Original_Unit', \
                   'Grainger_Attribute_Value', 'Normalized_Value', 'Normalized_Unit', 'Potential_Issue']]

    final_no_dupes = final_df.drop_duplicates(subset=['WS_Attribute_Name', 'Normalized_Value', 'Data_Type'])
    final_no_dupes = final_no_dupes[['Group_ID', 'PIM_Path', 'WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', \
                                     'WS_Node_Name', 'WS_SKU', 'Data_Type', 'WS_Attribute_Name', \
                                     'Grainger_Attribute_Value', 'Normalized_Value', 'Normalized_Unit', \
                                     'Potential_Issue']]

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_'+str(node_name)+'_MULTIVALUE_ISSUES.xlsx'  
    
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    final_no_dupes.to_excel (writer, sheet_name="Uniques", startrow=0, startcol=0, index=False)
    final_df.to_excel (writer, sheet_name="All MultiValue Issues", startrow=0, startcol=0, index=False)

    worksheet1 = writer.sheets['Uniques']
    worksheet2 = writer.sheets['All MultiValue Issues']

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

    worksheet1.set_column('J:J', 50, layout)
    worksheet1.set_column('K:K', 50, layout)
    worksheet1.set_column('M:M', 30, layout)

    col_widths = fd.get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width)

    worksheet2.set_column('P:P', 50, layout)
    worksheet2.set_column('R:R', 50, layout)
    worksheet2.set_column('S:S', 50, layout)
    worksheet2.set_column('U:U', 50, layout)

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

ws_df = pd.DataFrame()
temp_df_2 = pd.DataFrame()

init_time = time.time()
print('working...')

for node in search_data:
    start_time = time.time()
    ws_df = pd.DataFrame()

    print('k = ', node)

    if search_level == 'single':
        df = gws.gws_q(gws_attr_single, 'tprod."categoryId"', node)
        
    elif search_level == 'group':
        df = gws.gws_q(gws_attr_group, 'tprod."categoryId"', node)

    if df.empty == False:
        node_list = df['WS_Node_ID'].unique().tolist()

        print('{} nodes to process'.format(len(node_list)))

        for n in node_list:
            temp_df = df.loc[df['WS_Node_ID']== n]
            print('node {} : {} rows'.format(n, len(temp_df)))

            # split large df into 2 and print separately
            if len(temp_df) > 40000:
                count = 1
                num_lists = round(len(temp_df)/40000, 0)
                num_lists = int(num_lists)

                if num_lists == 1:
                    num_lists = 2
                
                print('processing values in {} batches'.format(num_lists))

                split_df = np.array_split(temp_df, num_lists)

                for object in split_df:
                    print('iteration {} of {}'.format(count, num_lists))
                    chunk_df = process_vals(object)
                    temp_df_2 = pd.concat([temp_df_2, chunk_df], axis=0, sort=False)

                    count += 1
                temp_df = temp_df_2
 
            else:
                temp_df = process_vals(temp_df)
            
            ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) 

        node_name = ws_df['WS_Category_Name'].unique().tolist()
        node_name = node_name[0]

        # drop rows that don't need review
        ws_df = ws_df[(ws_df['Potential_Issue'] != '')]
        ws_df = ws_df.sort_values(['Potential_Issue'], ascending=[True])

        # only pull grainger SKUs where there is a potential issue
        ws_df = get_STEP_vals(ws_df)

        # split large df into 2 and print separately
        if len(ws_df) > 300000:
            first_df = ws_df[:300000]
            rest_df = ws_df[300000:]

            data_out(first_df, node, node_name)
            node_name = node_name+'_2'
            data_out(rest_df, node, node_name)
            
        else:
            data_out(ws_df, node, node_name)

    print("--- segement: {} minutes ---".format(round((time.time() - start_time)/60, 2)))
    
print("--- {} minutes ---".format(round((time.time() - init_time)/60, 2)))