# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import numpy as np
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
from queries_WS import grainger_hier_query, grainger_discontinued_query, ws_hier_query
import file_data_GWS as fd
import pandas as pd
import settings_NUMERIC as settings
import time


gcom = GraingerQuery()
gws = GWSQuery()


GWS_cats="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "GWS_PIM_Path"
        , tax.ancestors[1] as "GWS_Category_ID"
        , tax.ancestor_names[1] as "GWS_Category_Name"
        , tax_att."categoryId" AS "GWS_Node_ID"
        , tax.name as "GWS_Node_Name"
        , tax_att.id as "GWS_Attr_ID"
        , tax_att.name as "GWS_Attribute_Name"
        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
   
    FROM  taxonomy_attribute tax_att

    INNER JOIN tax
        ON tax.id = tax_att."categoryId"
        
    FULL OUTER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
		
	WHERE {} = ANY({})

     """

grainger_attr_query="""
           	SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID
            , attr.DESCRIPTOR_NAME as Grainger_Attribute_Name

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat

            INNER JOIN PRD_DWH_VIEW_MTRL.ITEM_DESC_V AS item_attr
                ON  cat.CATEGORY_ID = item_attr.CATEGORY_ID
            
            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE attr.DESCRIPTOR_ID IN ({})
				AND cat.CATEGORY_ID IN ({})
            """
            
second_chance_query="""
           	SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , attr.DESCRIPTOR_ID as Grainger_Attr_ID
            , attr.DESCRIPTOR_NAME as Grainger_Attribute_Name

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat

            INNER JOIN PRD_DWH_VIEW_MTRL.ITEM_DESC_V AS item_attr
                ON  cat.CATEGORY_ID = item_attr.CATEGORY_ID
            
            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            WHERE {} IN ({})
            """
            
            
def attr_data(df, k):
    attr_list = df['STEP_Attr_ID'].unique().tolist()
    attr_list = ", ".join("'" + str(i) + "'" for i in attr_list)

    gr_att_list = gcom.grainger_q(grainger_attr_query, attr_list, k)
    gr_att_list = gr_att_list.drop_duplicates(ignore_index=True)
    return gr_att_list

def second_chance_atts(node):
    gr_att_list = gcom.grainger_q(second_chance_query, 'cat.CATEGORY_ID', node)
    
    return gr_att_list


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger L3 \n2. GWS ")
            if data_type in ['1']:
                data_type = 'grainger_query'
                break
            elif data_type in ['2']:
                data_type = 'gws_query'
                break
        except ValueError:
            print('Invalid search type')
        
    return data_type


#general output to xlsx file, used for the basic query
def data_out(df, batch=''):

    if df.empty == False:
        outfile = 'C:/Users/xcxg109/NonDriveFiles/Delta Attributes_'+str(batch)+'.xlsx'  

        df = df[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                 'Category_Name', 'Grainger_Attr_ID', 'Grainger_Attribute_Name', \
                 'GWS_Attribute_Name', 'GWS_Attr_ID', 'GWS_PIM_Path', 'GWS_Category_ID', \
                 'GWS_Category_Name', 'GWS_Node_ID', 'GWS_Node_Name', 'STEP_Category_ID', \
                 'STEP_Attr_ID']]        

        df = df.sort_values(['Segment_Name', 'Category_Name', 'Grainger_Attribute_Name'], ascending=[True, True, True])
        df_filter = df[df['GWS_Node_ID'].isna()]
            
        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')

        df_filter.to_excel (writer, sheet_name="STEP ONLY Attributes", startrow=0, startcol=0, index=False)
        df.to_excel (writer, sheet_name="ALL Attributes", startrow=0, startcol=0, index=False)

        worksheet1 = writer.sheets['STEP ONLY Attributes']
        worksheet2 = writer.sheets['ALL Attributes']

        col_widths = fd.get_col_widths(df_filter)
        col_widths = col_widths[1:]
        
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet1.set_column(i, i, width) 
        
        col_widths = fd.get_col_widths(df)
        col_widths = col_widths[1:]
        
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet2.set_column(i, i, width) 

        writer.save()
    
        
print('working....')
data_type = search_type()
#ask user for node number/SKU or pull from file if desired    
search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()

grainger_df = pd.DataFrame()

if data_type == 'grainger_query':
    count = 1
    num = len(search_data)
    
    for k in search_data:
        print ('{} : {}'.format(count, num))
        div = "'"+str(k)+"_DIV1'"

        # query WS to look for Grainger cat matches in pi_mappings table
        temp_df = gws.gws_q(GWS_cats, div, 'pi_mappings.step_category_ids')
        
        if temp_df.empty == False:
            # strip string elements from columns and change type to ints
            temp_df['STEP_Attr_ID'] = temp_df['STEP_Attr_ID'].str.replace('_ATTR', '')
            temp_df['STEP_Category_ID'] = temp_df['STEP_Category_ID'].str.replace('_DIV1', '')
            temp_df[['STEP_Category_ID', 'STEP_Attr_ID']] = temp_df[['STEP_Category_ID', 'STEP_Attr_ID']].apply(pd.to_numeric) 

            grainger_atts = attr_data(temp_df, k)
            
            if grainger_atts.empty == False:
                grainger_atts = grainger_atts.merge(temp_df, how="left", left_on=['Category_ID', 'Grainger_Attr_ID'],\
                                                                         right_on=['STEP_Category_ID', 'STEP_Attr_ID'])
 #               grainger_df = pd.concat([grainger_df, grainger_atts], axis=0)

        else:
            grainger_atts = second_chance_atts(k)
        
        grainger_df = pd.concat([grainger_df, grainger_atts], axis=0)
        print('k = ', k)
        count+=1
        
grainger_df = grainger_df.drop_duplicates()
grainger_df = grainger_df[grainger_df.Grainger_Attribute_Name != 'Item']
    
if len(grainger_df) > 900000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(grainger_df)/900000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(grainger_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, count)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(grainger_df)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
