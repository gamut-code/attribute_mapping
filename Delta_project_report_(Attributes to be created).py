# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 22:37:20 2021

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd
import time


gcom = GraingerQuery()
gws = GWSQuery()


grainger_value_query="""
           	SELECT item.MATERIAL_NO AS STEP_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID As Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SUPPLIER_NO
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS
            , item.PRICING_FLAG
            , item.PRICER_FIRST_EFFECTIVE_DATE
            , attr.DESCRIPTOR_ID as STEP_Attr_ID
            , attr.DESCRIPTOR_NAME AS STEP_Attribute_Name
            , item_attr.ITEM_DESC_VALUE AS STEP_Attribute_Value

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
             --   AND item.PM_CODE NOT IN ('R9')

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND attr.DELETED_FLAG = 'N'

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '') -- NOTE: blank RMC = MX only
                AND {} IN ({})
            """
            
            
ws_map="""
SELECT
    pi.gws_category_id as "GWS_Category_ID"
      , replace(array_to_string(pi.step_category_ids,', '), '_DIV1', '') as step_category_ids
    , gws_attr_id as "GWS_Attr_ID"
      , replace(array_to_string(pi.step_attribute_ids,', '), '_ATTR', '') as step_attribute_ids
      , tax_att.name as "GWS_Attribute_Name"

  FROM 
    (
    SELECT gws_category_id
      , step_category_ids
      , step_attribute_ids
--					, UNNEST(step_category_ids) AS step_cat_id
      , UNNEST(gws_attribute_ids) AS gws_attr_id
--					, UNNEST(step_attribute_ids) AS step_attr_id
    
    FROM pi_mappings
  ) pi
  
  FULL OUTER JOIN taxonomy_attribute tax_att
     ON pi.gws_attr_id = tax_att.id

WHERE {}= ANY (pi.step_category_ids)
    AND {} = ANY (step_attribute_ids)
"""
        

def data_out(df, batch=''):

    if df.empty == False:
        outfile = 'C:/Users/xcxg109/NonDriveFiles/Delta_Project_STEP_Values_'+str(batch)+'_.xlsx'

        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

        df.to_excel (writer, sheet_name="ALL STEP Att_Values", startrow=0, startcol=0, index=False)

        worksheet1 = writer.sheets['ALL STEP Att_Values']

        col_widths = fd.get_col_widths(df)
        col_widths = col_widths[1:]
        
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet1.set_column(i, i, width)

        writer.save()

    else:
        print('EMPTY DATAFRAME')


start_time = time.time()

#sku_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/step_only_skus_V2.csv')
sku_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/step_only_skus_2ndPASS.csv')
cats_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/Delta_Attributes.csv')

gr_sku_df = pd.DataFrame()
att_df = pd.DataFrame()
category_df = pd.DataFrame()

sku_list = sku_df['STEP_SKU'].tolist()

if len(sku_list)>4000:
    num_lists = round(len(sku_list)/4000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2

    print('running GWS SKUs in {} batches'.format(num_lists))

    size = round(len(sku_list)/num_lists, 0)
    size = int(size)

    div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

    for k  in range(0, len(div_lists)):
        print('batch {} of {}'.format(k+1, num_lists))
        gr_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
        temp_df = gcom.grainger_q(grainger_value_query, 'item.MATERIAL_NO', gr_skus)
        
        gr_sku_df = pd.concat([gr_sku_df, temp_df], axis=0, sort=False) 
        
else:
    gr_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    gr_sku_df = gcom.grainger_q(grainger_value_query, 'item.MATERIAL_NO', gr_skus)

cats = gr_sku_df['Category_ID'].unique().tolist()

for cat in cats:
    temp_df = gr_sku_df.loc[gr_sku_df['Category_ID']== cat]
    cat = "'" + str(cat) + "_DIV1'"
    
    atts = temp_df['STEP_Attr_ID'].unique().tolist()
    
    for att in atts:
        att = "'" + str(att) + "_ATTR'"
        
        temp_att_df = gws.gws_q(ws_map, cat, att)

        if temp_att_df.empty == False:
            att_df = pd.concat([att_df, temp_att_df], axis=0, sort=False) 

    if att_df.empty == False:
        category_df = pd.concat([category_df, att_df], axis=0, sort=False)

gr_sku_df = gr_sku_df.drop_duplicates()
final_df = gr_sku_df

category_df = category_df.drop_duplicates()

lst_col = 'step_category_ids'
x = category_df.assign(**{lst_col:category_df[lst_col].str.split(',')})
category_df = pd.DataFrame({col:np.repeat(x[col].values, x[lst_col].str.len()) \
              for col in x.columns.difference([lst_col])}).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]

category_df = category_df.astype({'step_attribute_ids': int, 'step_category_ids': int})

final_df = final_df.merge(category_df, how="left", left_on=['Category_ID', 'STEP_Attr_ID'], \
                                                    right_on=['step_category_ids', 'step_attribute_ids'])

final_df = final_df.drop(['step_category_ids', 'step_attribute_ids'], axis=1)

final_df = final_df.sort_values(by=['Segment_Name', 'Category_Name', 'STEP_SKU', \
                                    'STEP_Attribute_Name'], \
                                    ascending=[True, True, True, True])


if len(final_df) > 900000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(final_df)/900000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2

    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(final_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, count)
        count += 1
    
else:
    data_out(final_df)

gws_atts = final_df

gws_atts = gws_atts[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                 'Category_ID', 'Category_Name', 'STEP_Attr_ID', 'STEP_Attribute_Name', \
                 'GWS_Attr_ID', 'GWS_Category_ID', 'GWS_Attribute_Name']] 
gws_atts = gws_atts.drop_duplicates()

gws_atts = gws_atts[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', \
                 'Category_ID', 'Category_Name', 'STEP_Attr_ID', 'STEP_Attribute_Name', \
                 'GWS_Category_ID', 'GWS_Attr_ID', 'GWS_Attribute_Name']]
gws_atts = gws_atts.drop_duplicates()
    
no_match_df = gws_atts[gws_atts['GWS_Attr_ID'].isna()]
no_match_df = no_match_df.drop(['GWS_Category_ID', 'GWS_Attr_ID', 'GWS_Attribute_Name'], axis=1)

outfile = 'C:/Users/xcxg109/NonDriveFiles/Delta_Project_Attribute_Breakdown.xlsx'

writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

if no_match_df.empty == False:
    no_match_df.to_excel (writer, sheet_name="Delta Atts not in WS", startrow=0, startcol=0, index=False)
    worksheet1 = writer.sheets['Delta Atts not in WS']

    col_widths = fd.get_col_widths(no_match_df)
    col_widths = col_widths[1:]

    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width) 

gws_atts.to_excel (writer, sheet_name="Delta ALL Atts", startrow=0, startcol=0, index=False)
worksheet2 = writer.sheets['Delta ALL Atts']
    
col_widths = fd.get_col_widths(gws_atts)
col_widths = col_widths[1:]

for i, width in enumerate(col_widths):
    if width > 40:
        width = 40
    elif width < 10:
        width = 10
    worksheet2.set_column(i, i, width) 

writer.save()

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

    