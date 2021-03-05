# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 16:37:27 2021

@author: xcxg109
"""
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd
import pandas as pd
import numpy as np
import settings_NUMERIC as settings
import time

gcom = GraingerQuery()
gws = GWSQuery()


STEP_query="""
 SELECT item.MATERIAL_NO AS STEP_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SUPPLIER_NO
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS
            , item.PRICING_FLAG
            , item.PRICER_FIRST_EFFECTIVE_DATE
            
            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            FULL OUTER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
         		AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9')
        
            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '') -- NOTE: blank RMC = MX only
                AND {} IN ({})
"""

PIM_query="""
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
                        tax.ancestors || category."parentId",
                        tax.ancestor_names || parent_category.name
                FROM taxonomy_category as category
                JOIN tax on category."parentId" = tax.id
                JOIN taxonomy_category parent_category on category."parentId" = parent_category.id
                WHERE   category.deleted = false 
            )

            SELECT
                array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
                , tax.ancestors[1] as "PIM_Category_ID"
                , tax.ancestor_names[1] as "PIM_Category_Name"
                , {} AS "PIM_Node_ID"
                , tax.name as "PIM_Node_Name"
                , tprod."gtPartNumber" as "WS_SKU"
                , tprod.id as "PIM_SKU_ID"
                , tprod.status as "PIM_Status"
                , replace(array_to_string(step_category_ids,', '), '_DIV1', '') as STEP_Category_ID

            FROM taxonomy_product tprod

            INNER JOIN tax
                ON tax.id = tprod."categoryId"
                AND ({} = ANY(tax.ancestors))

            FULL OUTER JOIN pi_mappings
                ON tprod."categoryId" = pi_mappings.gws_category_id
            """
        
        

#general output to xlsx file, used for the basic query
def data_out(df, quer, batch=''):

    if df.empty == False:
        outfile = 'C:/Users/xcxg109/NonDriveFiles/STEP-PIM_'+str(batch)+'_HIER.xlsx'  

        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
        df.to_excel (writer, sheet_name="DATA", startrow=0, startcol=0, index=False)
        worksheet = writer.sheets['DATA']
        col_widths = fd.get_col_widths(df)
        col_widths = col_widths[1:]
        
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet.set_column(i, i, width) 
        writer.save()
    else:
        print('EMPTY DATAFRAME')
        
        
quer = 'HIER'

grainger_df = pd.DataFrame()
gws_df = pd.DataFrame()

# read in nodes from Grainger and WS
gr_data = input('Choose Grainger nodes file: ')
file_data = settings.get_file_data()
gr_data = [int(row[0]) for row in file_data[1:]]

gws_data = input ('Choose GWS file: ')
file_data = settings.get_file_data()
gws_data = [int(row[0]) for row in file_data[1:]]


print('working....')
start_time = time.time()

print('Grainger nodes')
for k in gr_data:
    print(k)
    temp_gr = gcom.grainger_q(STEP_query, 'cat.SEGMENT_ID', k)
    grainger_df = pd.concat([grainger_df, temp_gr], axis=0)

print('\n\nGWS nodes')
for k in gws_data:
    print(k)
    temp_gws = gws.gws_q(PIM_query, 'tprod."categoryId"', k)
    gws_df = pd.concat([gws_df, temp_gws], axis=0)

grainger_df.drop_duplicates()
gws_df = gws_df.drop_duplicates(ignore_index=True)

final_df = grainger_df.merge(gws_df, how="left", left_on="STEP_SKU", right_on='WS_SKU')
no_match_df = final_df[final_df['WS_SKU'].isna()]

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
        
        data_out(object, quer, count)
        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(final_df, quer)

outfile = 'C:/Users/xcxg109/NonDriveFiles/STEP_only_SKUs.xlsx'  

writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
no_match_df.to_excel (writer, sheet_name="DATA", startrow=0, startcol=0, index=False)
worksheet = writer.sheets['DATA']
col_widths = fd.get_col_widths(no_match_df)
col_widths = col_widths[1:]
        
for i, width in enumerate(col_widths):
    if width > 40:
        width = 40

    elif width < 10:
        width = 10

    worksheet.set_column(i, i, width) 

writer.save()


print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
