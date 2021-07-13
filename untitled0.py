# -*- coding: utf-8 -*-
#Created on Thu Apr 29 21:14:51 2021

#author: xcxg109

import pandas as pd
import numpy as np
import settings_NUMERIC as settings
import time
from GWS_query import GWSQuery

gws = GWSQuery()
pd.options.mode.chained_assignment = None


ws_attr_values="""
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
        tax.ancestors[1] as "WS_Parent_ID"
        , tax.ancestor_names[1] as "WS_Parent"
        , tprod."categoryId" AS "WS_Leaf_ID"
        , tax.name as "WS_Leaf_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att."allowedValues" as "Allowed_Values"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
        , tprodvalue.unit as "Original_Unit"
        , tax_att.rank as "Rank"
        , tax_att.priority as "Priority"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
--        AND (8086 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
--        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att.deleted = 'false'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        AND tprodvalue.deleted = 'false'
 
      WHERE {} IN ({})  
--    WHERE tax_att."dataType" = 'number'
--      WHERE tax_att."allowedValues" IS NOT NULL
        """


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def data_out(final_df, cat, batch=''):
    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(cat)+'_Attribute_Vals_'+str(batch)+'.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter', options={'strings_to_urls': False})
    workbook  = writer.book

    final_df.to_excel (writer, sheet_name="Numbers", startrow=0, startcol=0, index=False)
    worksheet1 = writer.sheets['Numbers']
        
    col_widths = get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    writer.save()

    
start_time = time.time()
print('working...')

ws_df = pd.DataFrame()

search_data = input('Input Blue node ID or hit ENTER to read from file: ')

if search_data != "":
    search_data = search_data.strip()
    search_data = [search_data]

else:
    file_data = settings.get_file_data()
    search_data = [row[0] for row in file_data[1:]]

for cat in search_data:
    print('node = ', cat)
    
    temp_df = gws.gws_q(ws_attr_values, 'tprod."categoryId"', cat)
#    temp_df = gws.gws_q(ws_attr_values, 'tax.ancestors[1]', cat)
    
    ws_df =  pd.concat([ws_df, temp_df], axis=0, sort=False) 

if len(ws_df) > 900000:
    count = 1
		# split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(ws_df)/900000, 0)
    num_lists = int(num_lists)
    if num_lists == 1:
        num_lists = 2
    print('creating {} output files'.format(num_lists))
    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(ws_df, num_lists)
    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        data_out(object, 3309, count)
        count += 1
else:
    data_out(ws_df, 3309)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))