l th# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 20:34:02 2020

@author: xcxg109
"""

import pandas as pd
import numpy as np
import WS_query_code as q
from grainger_query import GraingerQuery
import file_data_GWS as fd
import settings_NUMERIC as settings
import time


pd.options.mode.chained_assignment = None

gcom = GraingerQuery()


basic_hier_query="""
           	SELECT cat.CATEGORY_ID AS Category_ID
			, item.SUPPLIER_NO AS Supplier_ID
            , item.MATERIAL_NO AS Grainger_SKU

            FROM PRD_DWH_VIEW_MTRL.ITEM_V AS item
              
            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item.CATEGORY_ID
                AND item.DELETED_FLAG = 'N'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9', 'R4')
                AND item.PM_CODE NOT IN ('UA','UB','UC','UD','UE','UF','UG','UH','UJ','UK','UL','UM','UN','UT','UZ')            
                AND item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
                
            WHERE {} IN ({}) 
            """
            
grainger_attr_query="""
           	SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.MATERIAL_NO AS Grainger_SKU
            , item.PM_CODE AS PM_Code
            , item.SALES_STATUS AS Sales_Status
            , item.RELATIONSHIP_MANAGER_CODE AS Relationship_Mgr_Code
            , supplier.SUPPLIER_NO AS Supplier_ID
            , supplier.SUPPLIER_NAME AS Supplier_Name
            , attr.DESCRIPTOR_ID as Attr_ID
            , attr.DESCRIPTOR_NAME as Attribute_Name
            , item_attr.ITEM_DESC_VALUE as Attribute_Value
            , cat_desc.ENDECA_RANKING AS Endeca_Ranking

            FROM PRD_DWH_VIEW_MTRL.ITEM_DESC_V AS item_attr

            INNER JOIN PRD_DWH_VIEW_MTRL.ITEM_V AS item
                ON 	item_attr.MATERIAL_NO = item.MATERIAL_NO
                AND item.DELETED_FLAG = 'N'
                AND item_attr.LANG = 'EN'
                AND item.PM_CODE NOT IN ('R9', 'R4')
                AND item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
                AND item.PM_CODE NOT IN ('UA','UB','UC','UD','UE','UF','UG','UH','UJ','UK','UL','UM','UN','UT','UZ')
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item_attr.DELETED_FLAG = 'N'

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item_attr.CATEGORY_ID

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND attr.DESCRIPTOR_NAME NOT IN ('Green Certification or Other Recognition', 'Green Environmental Attribute')
                
            FULL OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                on prod.MATERIAL = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO

            WHERE {} IN ({})
            """
            
sales_2019="""
            SELECT DISTINCT
                TRIM (prod.material) AS Grainger_SKU
           --     , COALESCE (inv.sales,0) as Sales
           --     , COALESCE (inv.GP,0) as gp
                , COALESCE (inv.cogs,0) as COGS
            
            FROM PRD_DWH_VIEW_LMT.material_v as prod
            
            LEFT JOIN
            
              (SELECT DISTINCT
                  si.material
--                , SUM (si.subtotal_2) as Sales
--                , SUM (si.subtotal_2) - SUM (si.source_cost) AS gp
                , SUM (si.source_cost) AS cogs
--                , ROUND(COUNT(si.material),4) AS txn_lines
--                , COUNT (DISTINCT si.sold_to) AS buying_accounts
--                , SUM (si.inv_qty) AS units
            
              FROM PRD_DWH_VIEW_LMT.sales_invoice_v AS si
            
            /****************************************************************************/
            /* Update BETWEEN dates here to customize period of time for tracking sales */
            /****************************************************************************/
            
              WHERE si.fiscper BETWEEN 2019001 AND 2019012
                    AND si.division = '01'
            
              GROUP BY si.material
              ) AS inv
            
            ON prod.material = inv.material
            
            AND {} IN ({})              
"""

def get_sales(df):
    # create a list of unique SKUs and run through the 2019 sales report for COGS
    sku_list = df['Grainger_SKU'].unique().tolist()
    gr_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
    print('sku count = ', len(sku_list))
    sales = gcom.grainger_q(sales_2019, 'prod.material', gr_skus)

    return sales


def get_category_fill_rate(cat_df):
    browsable_skus = cat_df
    
    #calculate fill rates at the attribute / category level    
    total = browsable_skus['Grainger_SKU'].nunique()
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Attr_ID'])
    
    browsable_skus['Category_Fill_Rate_%'] = (browsable_skus.groupby('Attr_ID')['Attr_ID'].transform('count')/total)*100
    browsable_skus['Category_Fill_Rate_%'] = browsable_skus['Category_Fill_Rate_%'].map('{:,.2f}'.format)
    
    fill_rate_cat = pd.DataFrame(browsable_skus.groupby(['Attr_ID'])['Category_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                                     'Category_Name', 'Attr_ID', 'Attribute_Name', 'Endeca_Ranking']]
    browsable_skus = browsable_skus.drop_duplicates(subset='Attr_ID')
    
    fill_rate_cat = fill_rate_cat.merge(browsable_skus, how= "inner", on=['Attr_ID'])
    fill_rate_cat['Category_Fill_Rate_%'] = fill_rate_cat['Category_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate_cat = fill_rate_cat.merge(browsable_skus, how= "inner", on=['Segment_ID', 'Segment_Name', 'Family_ID', \
                                                'Family_Name', 'Category_ID', 'Category_Name', 'Attr_ID', \
                                                'Attribute_Name', 'Endeca_Ranking'])

    return fill_rate_cat
    
    
def get_supplier_fill_rate(df):
    # note: df here is already segregated by category_ID
    browsable_skus = df
    
    #calculate fill rates at the attribute / category level    
    total = browsable_skus['Grainger_SKU'].nunique()
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Attr_ID'])
    
    browsable_skus['Supplier_Fill_Rate_%'] = (browsable_skus.groupby('Attr_ID')['Attr_ID'].transform('count')/total)*100
    browsable_skus['Supplier_Fill_Rate_%'] = browsable_skus['Supplier_Fill_Rate_%'].map('{:,.2f}'.format)
    
    fill_rate_sup = pd.DataFrame(browsable_skus.groupby(['Attr_ID'])['Supplier_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['Attr_ID']].drop_duplicates(subset='Attr_ID')
    
    fill_rate_sup = fill_rate_sup.merge(browsable_skus, how= "inner", on=['Attr_ID'])
    fill_rate_sup['Supplier_Fill_Rate_%'] = fill_rate_sup['Supplier_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate_sup = fill_rate_sup.merge(browsable_skus, how= "inner", on=['Attr_ID'])

    return fill_rate_sup


def data_out(df, atts_df, batch=''):
    # output for sku-based pivot table
    sku_data = df[['Supplier_Parent_Group', 'Supplier_ID', 'Supplier_Name', 'Segment_ID', 'Segment_Name', \
                 'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', 'Grainger_SKU', 'PM_Code', \
                 'Sales_Status', 'Relationship_Mgr_Code', '2019_COGS']]        
    sku_data = sku_data.drop_duplicates(subset=['Grainger_SKU'])
    sku_data = sku_data.rename(columns={'Grainger_SKU':'Material_No'})

    fill = atts_df[['Supplier_Parent_Group', 'Supplier_ID', 'Supplier_Name', 'Segment_ID', 'Segment_Name', \
               'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', 'Attr_ID', 'Attribute_Name', \
               'Endeca_Ranking', 'Supplier_Fill_Rate_%', 'Category_Fill_Rate_%']] 
    fill = fill.drop_duplicates(subset=['Category_ID', 'Attr_ID'])
    fill = fill.sort_values(by=['Segment_Name', 'Family_Name', 'Category_Name', 'Endeca_Ranking'])
        
    outfile = 'C:/Users/xcxg109/NonDriveFiles/SUPPLIER_REPORT_'+str(batch)+'_.xlsx'

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    sku_data.to_excel (writer, sheet_name="SKU Data", startrow=0, startcol=0, index=False)
    fill.to_excel(writer, sheet_name='Attribute Fill Rates', startrow =0, startcol=0, index=False)

    worksheet1 = writer.sheets['Attribute Fill Rates']
    worksheet2 = writer.sheets['SKU Data']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    col_widths = fd.get_col_widths(fill)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    col_widths = fd.get_col_widths(sku_data)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width)

    writer.save()


filename = settings.choose_file()

temp_all_cats = pd.DataFrame()
temp_all_atts = pd.DataFrame()
grainger_df = pd.DataFrame()
grainger_atts = pd.DataFrame()

init_time = time.time()

parent_df = pd.read_csv(filename)
parent_df = parent_df.rename(columns={'Supplier':'Supplier_ID', \
                                      'Supplier Name':'Supplier_Name', \
                                      'Supplier Parent Group':'Supplier_Parent_Group'})
    

suppliers = parent_df['Supplier_ID'].unique().tolist()
loop_count = 1

delim = '|'    
sales_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/COGS_by_SKU.txt', delimiter=delim, error_bad_lines=False)
sales_df = sales_df.rename(columns={'COGS':'2019_COGS'})

for sup in suppliers:
    start_time = time.time()
    
    # get basic hierarchy data and combine with the supplier names/IDs
    categories_df = gcom.grainger_q(basic_hier_query, 'item.SUPPLIER_NO', sup)
#    supplier_df = supplier_df.merge(parent_df, how= "inner", on=['Supplier_ID'])
    
    # pull all L3s for the supplier and get attribute data on each node
    cats = categories_df['Category_ID'].unique().tolist()

    print('Supplier ID {} -- {} of {}: {} categories'.format(sup, loop_count, len(suppliers), len(cats)))

    # get fill rate and SKU counts by category for supplier
    for cat in cats:
        print('cat - ', cat)

        # temp_df is the ENTIRE category, supplier_df is filtered by supplier ID        
        temp_df = gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', cat)
        temp_df = temp_df.merge(parent_df, how="outer", on=['Supplier_ID', 'Supplier_Name'])

        # get category fill rates first because supplier may not have all active attributes in a category
        fill_category = get_category_fill_rate(temp_df)
 
        # create filtered supplier_df and get fill rates specific to supplier
        supplier_df = temp_df.loc[temp_df['Supplier_ID']== sup]        
        fill_supplier = get_supplier_fill_rate(supplier_df)

        # set up the fill_supplier rows for a merge with temp_df -- this ensures that attributes active in the node 
        # but not populated by the supplier are still included in the the df
        sup_name = supplier_df['Supplier_Name'].unique().tolist()
        sup_parent = supplier_df['Supplier_Parent_Group'].unique().tolist()
        sup_parent = [supt for supt in sup_parent if str(supt) != 'nan']    # remove nans from list
        
        fill_category['Supplier_ID'] = sup
        fill_category['Supplier_Parent_Group'] = sup_parent[0]
        fill_category['Supplier_Name'] = sup_name[0]

        temp_df = temp_df.merge(fill_category, how="outer", on=['Supplier_Parent_Group', 'Supplier_ID', 'Supplier_Name', \
                                        'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                                        'Category_Name', 'Attr_ID', 'Attribute_Name', 'Endeca_Ranking']) 
#        temp_df = pd.concat([fill_category, temp_df], sort=False)
        
        temp_df = temp_df.merge(fill_supplier, how="outer", on=['Attr_ID'])
        temp_df[['Supplier_Fill_Rate_%']] = temp_df[['Supplier_Fill_Rate_%']].fillna(value='0')        

        temp_df = temp_df.sort_values(by=['Category_ID', 'Attr_ID', 'Category_Fill_Rate_%', 'Supplier_Fill_Rate_%'])
        temp_df = temp_df.drop_duplicates('Attr_ID', keep='first')

        temp_all_cats = pd.concat([temp_all_cats, supplier_df], axis=0, sort=False) 
        temp_all_atts = pd.concat([temp_all_atts, temp_df], axis=0, sort=False)
        
    grainger_df = pd.concat([grainger_df, temp_all_cats], axis=0, sort=False) 
    grainger_atts = pd.concat([grainger_atts, temp_all_atts], axis=0, sort=False) 

    print("--- segment: {} minutes ---".format(round((time.time() - start_time)/60, 2)))
    loop_count += 1
        
grainger_df = grainger_df.merge(sales_df, how= "inner", on=['Grainger_SKU'])

if len(grainger_df) > 300000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(grainger_df)/300000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(grainger_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, grainger_atts, count)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(grainger_df, grainger_atts)

print("--- total time: {} minutes ---".format(round((time.time() - init_time)/60, 2)))

#temp_df.to_csv('C:/Users/xcxg109/NonDriveFiles/temp_df.csv')

#grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/cogs.csv')