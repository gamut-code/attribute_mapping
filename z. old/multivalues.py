# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 09:28:25 2020
original multivalue report
@author: xcxg109
"""

import pandas as pd
from GWS_query import GWSQuery
import file_data_GWS as fd
import time
import settings_NUMERIC as settings
from grainger_query import GraingerQuery
import WS_query_code as q

pd.options.mode.chained_assignment = None

gws = GWSQuery()
gcom = GraingerQuery()


gws_values_single="""
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
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tprodvalue.id
        , tprodvalue."productId"
        , tprodvalue."id_migration"
        , tax_att.id as "WS_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tax_att."dataType" as "Data_Type"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."multiValue" = 'TRUE'
        AND {} IN ({})
        """

gws_values_group="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "WS_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tprodvalue.id
        , tprodvalue."productId"
        , tprodvalue."id_migration"
        , tax_att.name as "WS_Attribute_Name"
        , tax_att."dataType" as "Data_Type"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."multiValue" = 'TRUE'
        """


def get_col_widths(df):
    # find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]
   
     
def concat_values(df):
    # build string of all values for stats front page
    temp_final = pd.DataFrame()
    all_vals = pd.DataFrame()
    
    func_df = df.copy()
    func_df['Count'] =1

    atts = func_df['WS_Attribute_Name'].unique().tolist()
    
    vals = pd.DataFrame(func_df.groupby(['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'Original_Value'])['Count'].sum())
    vals = vals.reset_index()

    for attribute in atts:
        temp_att_df = vals.loc[vals['WS_Attribute_Name']== attribute]
        temp_att_df['List_of_Values'] = ''

        skus = temp_att_df['WS_SKU'].unique().tolist()

        for sku in skus:
            temp_sku_df = temp_att_df.loc[temp_att_df['WS_SKU']== sku]
            
            if len(temp_sku_df.index) > 1:
                temp_sku_df['List_of_Values'] = '; '.join(item for item in temp_sku_df['Original_Value'])
                temp_sku_df['#_Values'] = len(temp_sku_df.index)
                
                temp_final = pd.concat([temp_final, temp_sku_df], axis=0)
                
        all_vals = pd.concat([all_vals, temp_final], axis=0)

    if all_vals.empty == False:
        all_vals = all_vals[['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'List_of_Values', '#_Values']]
        all_vals = all_vals.drop_duplicates(subset=['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU'])

    return all_vals


ws_df = pd.DataFrame()
att_vals = pd.DataFrame()

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

print('working...')

# read in grainger data
allCATS_df = q.get_att_values()            

for node in search_data:
    start_time = time.time()

    if search_level == 'single':
        init_ws_df = gws.gws_q(gws_values_single, 'tprod."categoryId"', node)
    elif search_level == 'group':
        init_ws_df = gws.gws_q(gws_values_group, 'tprod."categoryId"', node)
        
    print('k = ', node)
    
    if init_ws_df.empty == False:
        node_names = init_ws_df['WS_Node_Name'].unique().tolist()
        
        for n in node_names:
            temp_df = init_ws_df.loc[init_ws_df['WS_Node_Name']== n]
            temp_df['Count'] =1

            temp_grainger = allCATS_df.loc[allCATS_df['STEP_Category_Name']==n]
            
            if temp_grainger.empty==False:
                print(temp_grainger['STEP_Category_Name'].unique())
                temp_grainger = temp_grainger[['WS_SKU', 'WS_Attribute_Name', 'Grainger_Attribute_Value']]
                               
                cols = ['WS_SKU', 'WS_Attribute_Name']
                temp_df = temp_df.join(temp_grainger.set_index(cols), on=cols)

                ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False)

                temp_df = concat_values(temp_df)                
                att_vals = pd.concat([att_vals, temp_df], axis=0)

            else:                
                print('WS node = ', n)
                ws_skus = temp_df['WS_SKU'].unique().tolist()            
                print('{} : {} SKUs'.format(n, len(ws_skus)))
        
                for sku in ws_skus:
                    temp_ws = init_ws_df.loc[init_ws_df['WS_SKU']== sku]
                    temp_grainger = allCATS_df.loc[allCATS_df['WS_SKU']== sku]
                    print('found {} in Grainger node {}'.format(sku, temp_grainger['STEP_Category_Name'].unique()))
                    temp_grainger = temp_grainger[['WS_SKU', 'WS_Attribute_Name', 'Grainger_Attribute_Value']]
            
                    cols = ['WS_SKU', 'WS_Attribute_Name']
                    temp_ws = temp_ws.join(temp_grainger.set_index(cols), on=cols)
        
                    ws_df = pd.concat([ws_df, temp_ws], axis=0, sort=False)  

                temp_ws = concat_values(temp_ws)                
                att_vals = pd.concat([att_vals, temp_ws], axis=0)

        if att_vals.empty==False:
#            file = 'C:/Users/xcxg109/NonDriveFiles/att_vals_'+str(n)+'.csv'
#            att_vals.to_csv(file)                
            
            ws_df = pd.merge(ws_df, att_vals, on=['WS_Node_Name', 'WS_SKU', 'WS_Attribute_Name'])

        if 'List_of_Values' in ws_df.columns:
            ws_df = ws_df.sort_values(['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'Original_Value'], \
                            ascending=[True, True, True, True])

            ws_upload = ws_df[['WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', 'List_of_Values', 'Grainger_Attribute_Value']]
            ws_upload['Count'] = 1
            ws_upload = pd.DataFrame(ws_upload.groupby(['WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', 'List_of_Values', 'Grainger_Attribute_Value'])['Count'].sum())
            ws_upload = ws_upload.reset_index()
            ws_upload = ws_upload.drop(['Count'], axis=1)
                                    
            ws_no_dupes = ws_df.drop_duplicates(subset=['WS_Attribute_Name', 'List_of_Values'])
            ws_no_dupes = ws_no_dupes[['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'Data_Type', 'List_of_Values', '#_Values']]
            ws_no_dupes = ws_no_dupes.rename(columns={'WS_SKU':'Example SKU'})

            ws_no_dupes = ws_df.drop_duplicates(subset=['WS_Attribute_Name', 'List_of_Values'])
            ws_no_dupes = ws_no_dupes[['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'List_of_Values', '#_Values']]
            ws_no_dupes = ws_no_dupes.rename(columns={'WS_SKU':'Example SKU'})

            ws_df = ws_df.drop(['Count', 'List_of_Values'], axis=1)
      
            outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_multivalues.xlsx'  
            writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
            workbook  = writer.book

            ws_no_dupes.to_excel (writer, sheet_name="Stats", startrow=0, startcol=0, index=False)
            ws_upload.to_excel (writer, sheet_name="Upload Sheet", startrow=0, startcol=0, index=False)
            ws_df.to_excel (writer, sheet_name="MultiValues", startrow=0, startcol=0, index=False)

            worksheet1 = writer.sheets['Stats']
            worksheet2 = writer.sheets['Upload Sheet']
            worksheet3 = writer.sheets['MultiValues']

            layout = workbook.add_format()
            layout.set_text_wrap('text_wrap')
            layout.set_align('left')

            col_widths = get_col_widths(ws_no_dupes)
            col_widths = col_widths[1:]
    
            for i, width in enumerate(col_widths):
                if width > 40:
                    width = 40
                elif width < 10:
                    width = 10
                worksheet1.set_column(i, i, width)

            worksheet1.set_column('D:D', 70, layout)

            col_widths = get_col_widths(ws_upload)
            col_widths = col_widths[1:]
    
            for i, width in enumerate(col_widths):
                if width > 40:
                    width = 40
                elif width < 10:
                    width = 10
                worksheet2.set_column(i, i, width)

            col_widths = get_col_widths(ws_df)
            col_widths = col_widths[1:]
    
            worksheet2.set_column('F:F', 70, layout)

            for i, width in enumerate(col_widths):
                if width > 40:
                    width = 40
                elif width < 10:
                    width = 10
                worksheet3.set_column(i, i, width)

            worksheet3.set_column('M:M', 70, layout)
            worksheet3.set_column('O:O', 70, layout)

            writer.save()
        
        else:
            print('\n{} No true multivalues'.format(node))
    else:
        print('{} No attribute data'.format(node))

    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

