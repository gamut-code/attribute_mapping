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
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , flat.Terminal_Node_ID AS Yellow_Terminal_Node_ID
            , flat.Terminal_Node_Name AS Yellow_Terminal_Node_Name
            , item.MATERIAL_NO AS Grainger_SKU
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID
            , attr.DESCRIPTOR_NAME as Grainger_Attribute_Name

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

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            INNER JOIN PRD_DWH_VIEW_LMT.Prod_Yellow_Heir_Class_View AS yellow
                ON yellow.PRODUCT_ID = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.Yellow_Heir_Flattend_view AS flat
                ON yellow.PROD_CLASS_ID = flat.Heir_End_Class_Code
                                
            WHERE {} IN ({})
            """
            
            
gcom = GraingerQuery()


def data_out(df):

    if df.empty == False:
        outfile = 'C:/Users/xcxg109/NonDriveFiles/STEP_Blue_Yellow_lastChance.xlsx'

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
cats_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/hier_test.csv')

cats = cats_df['Terminal_Node_ID'].unique().tolist()

category_gr = pd.DataFrame()
count = 1

for cat in cats:
    cat = "'" + str(cat) + "'"
    print('{}. {}'.format(count, cat))
    temp_gr = gcom.grainger_q(grainger_cat_query, 'Terminal_Node_ID', cat)

    if temp_gr.empty == False:
        category_gr = pd.concat([category_gr, temp_gr], axis=0, sort=False) 

    count += 1            
final_df = category_gr
final_df = final_df.drop_duplicates()

if final_df.empty == False:
    final_df = final_df.sort_values(by=['Segment_Name', 'Category_Name'], ascending=[True, True])

    data_out(final_df)
else:
    print('EMPTY DATAFRAME')