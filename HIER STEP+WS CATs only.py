# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import numpy as np
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import settings_NUMERIC as settings
import file_data_GWS as fd
import pandas as pd
import time

grainger_cat_query="""
       SELECT DISTINCT (cat.CATEGORY_ID)
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID As Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat

            WHERE {} IN ({})
            """
            
            
ws_map="""
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

            SELECT DISTINCT (tprod."categoryId")
                , array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
                , tax.ancestors[1] as "PIM_Category_ID"
                , tax.ancestor_names[1] as "PIM_Category_Name"
                , {} AS "PIM_Leaf_Node_ID"
                , tax.name as "PIM_Node_Name"
                , replace(array_to_string(pi.step_category_ids,', '), '_DIV1', '') as step_category_ids

  FROM 
    (
    SELECT gws_category_id
      , step_category_ids
    
    FROM pi_mappings
  ) pi
  
  FULL OUTER JOIN taxonomy_product tprod
      ON tprod."categoryId" = pi.gws_category_id

   INNER JOIN tax
       ON tax.id = tprod."categoryId"

    WHERE {}= ANY (pi.step_category_ids)
"""

gcom = GraingerQuery()
gws = GWSQuery()


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger Blue (node) \n2. GWS ")
            if data_type in ['1', 'node', 'Node', 'NODE', 'blue', 'Blue', 'BLUE', 'b', 'B']:
                data_type = 'grainger_query'
                break
            elif data_type in ['2', 'gws', 'Gws', 'GWS', 'g', 'G']:
                data_type = 'gws_query'
                break
        except ValueError:
            print('Invalid search type')
        
    return data_type


#function to get node/SKU data from user or read from the data.csv file
def data_in(data_type, directory_name):
#    type_list = ['Node', 'SKU']
    
    if data_type == 'grainger_query':
        search_data = input('Input Blue node ID or hit ENTER to read from file: ')
    elif data_type == 'gws_query':
        search_data = input ('Input GWS terminal node ID or ENTER to read from file: ')
        
    if search_data != "":
        search_data = search_data.strip()
        search_data = [search_data]
        return search_data

    else:
        file_data = settings.get_file_data()

        search_data = [int(row[0]) for row in file_data[1:]]
        return search_data


def data_out(df):

    if df.empty == False:
        outfile = 'C:/Users/xcxg109/NonDriveFiles/STEP_WS_Cats.xlsx'

        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

        df.to_excel (writer, sheet_name="Category", startrow=0, startcol=0, index=False)

        worksheet1 = writer.sheets['Category']

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




print('working....')
data_type = search_type()
#ask user for node number/SKU or pull from file if desired    
search_data = data_in(data_type, settings.directory_name)

start_time = time.time()

category_gr = pd.DataFrame()
category_gws = pd.DataFrame()

for cat in search_data:

    temp_gr = gcom.grainger_q(grainger_cat_query, 'cat.CATEGORY_ID', cat)

    if temp_gr.empty == False:
        category_gr = pd.concat([category_gr, temp_gr], axis=0, sort=False) 
    
    cat = "'" + str(cat) + "_DIV1'"
    temp_gws = gws.gws_q(ws_map, 'tprod."categoryId"', cat)

    if temp_gws.empty == False:
        category_gws = pd.concat([category_gws, temp_gws], axis=0, sort=False) 
            
lst_col = 'step_category_ids'
x = category_gws.assign(**{lst_col:category_gws[lst_col].str.split(',')})
category_gws = pd.DataFrame({col:np.repeat(x[col].values, x[lst_col].str.len()) \
              for col in x.columns.difference([lst_col])}).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()]
        
category_gws = category_gws.astype({'step_category_ids': int})

final_df = category_gr.merge(category_gws, how="left", left_on=['Category_ID'], \
                                                    right_on=['step_category_ids'])

final_df = final_df.drop_duplicates()
final_df = final_df.drop(['CATEGORY_ID', 'categoryId', 'step_category_ids'], axis=1)

final_df = final_df.sort_values(by=['Segment_Name', 'Category_Name'], ascending=[True, True])

data_out(final_df)
