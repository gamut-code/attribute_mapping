# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 20:34:02 2020

@author: xcxg109
"""

import pandas as pd
import WS_query_code as q
from grainger_query import GraingerQuery
import file_data_GWS as fd
import settings_NUMERIC as settings
import time

gcom = GraingerQuery()


basic_hier_query="""
           	SELECT cat.CATEGORY_ID AS Category_ID
			, item.SUPPLIER_NO

            FROM PRD_DWH_VIEW_MTRL.ITEM_V AS item
              
            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item.CATEGORY_ID

            WHERE 
				{} IN ({})          
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
                AND item_attr.DELETED_FLAG = 'N'
                AND item_attr.LANG = 'EN'
                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
                AND item.PM_CODE NOT IN ('R9', 'R4')

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
                ON cat.CATEGORY_ID = item_attr.CATEGORY_ID
                AND item_attr.DELETED_FLAG = 'N'

            INNER JOIN PRD_DWH_VIEW_MTRL.CAT_DESC_V AS cat_desc
                ON cat_desc.CATEGORY_ID = item_attr.CATEGORY_ID
                AND cat_desc.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID
                AND cat_desc.DELETED_FLAG='N'

            INNER JOIN PRD_DWH_VIEW_MTRL.MAT_DESCRIPTOR_V AS attr
                ON attr.DESCRIPTOR_ID = item_attr.DESCRIPTOR_ID

            FULL OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
                on prod.MATERIAL = item.MATERIAL_NO

            FULL OUTER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
                ON prod.vendor = supplier.SUPPLIER_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'WV', 'WG')
                AND item.PM_CODE NOT IN ('UA','UB','UC','UD','UE','UF','UG','UH','UJ','UK','UL','UM','UN','UT','UZ')
                AND attr.DESCRIPTOR_NAME NOT IN ('Green Certification or Other Recognition', 'Green Environmental Attribute')
                AND {} IN ({})
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


def get_category_fill_rate(cat_id):
    category_df = gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', cat)

    fill_rate = pd.DataFrame()

    browsable_skus = category_df
    
    #calculate fill rates at the attribute / category level    
    total = browsable_skus['Grainger_SKU'].nunique()
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Attribute_Name'])
    
    browsable_skus['Category_Fill_Rate_%'] = (browsable_skus.groupby('Attribute_Name')['Attribute_Name'].transform('count')/total)*100
    browsable_skus['Category_Fill_Rate_%'] = browsable_skus['Category_Fill_Rate_%'].map('{:,.2f}'.format)
    
    fill_rate = pd.DataFrame(browsable_skus.groupby(['Attribute_Name'])['Category_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                                     'Category_Name', 'Attr_ID', 'Attribute_Name', 'Endeca_Ranking']]
    browsable_skus = browsable_skus.drop_duplicates(subset='Attribute_Name')
    
    fill_rate = fill_rate.merge(browsable_skus, how= "outer", on=['Attribute_Name'])
    fill_rate['Category_Fill_Rate_%'] = fill_rate['Category_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Segment_ID', 'Segment_Name', 'Family_ID', \
                                                'Family_Name', 'Category_ID', 'Category_Name', 'Attr_ID', \
                                                'Attribute_Name', 'Endeca_Ranking'])

    return fill_rate

    
    
def get_supplier_fill_rate(df):
    # note: df here is already segregated by category_ID
    fill_rate = pd.DataFrame()
    
    browsable_skus = df
    
    #calculate fill rates at the attribute / category level    
    total = browsable_skus['Grainger_SKU'].nunique()
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Attr_ID'])
    
    browsable_skus['Supplier_Fill_Rate_%'] = (browsable_skus.groupby('Attr_ID')['Attr_ID'].transform('count')/total)*100
    browsable_skus['Supplier_Fill_Rate_%'] = browsable_skus['Supplier_Fill_Rate_%'].map('{:,.2f}'.format)
    
    fill_rate = pd.DataFrame(browsable_skus.groupby(['Attr_ID'])['Supplier_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['Attr_ID']].drop_duplicates(subset='Attr_ID')
    
    fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Attr_ID'])
    fill_rate['Supplier_Fill_Rate_%'] = fill_rate['Supplier_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Attr_ID'])

    return fill_rate


def data_out(df, atts_df):
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
        
    outfile = 'C:/Users/xcxg109/NonDriveFiles/SUPPLIER_REPORT.xlsx'  
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


start_time = time.time()

    
grainger_df = pd.DataFrame()
grainger_atts = pd.DataFrame()
temp_all_cats = pd.DataFrame()
temp_df_fill = pd.DataFrame()
df_fill = pd.DataFrame()


filename = settings.choose_file()
parent_df = pd.read_csv(filename)

parent_df = parent_df.rename(columns={'Supplier':'Supplier_ID', \
                                      'Supplier Name':'Supplier_Name', \
                                      'Supplier Parent Group':'Supplier_Parent_Group'})

suppliers = parent_df['Supplier_ID'].unique().tolist()
loop_count = 1

print('working...')

delim = '|'    
sales_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/code/COGS_by_SKU.txt', delimiter=delim, error_bad_lines=False)
sales_df = sales_df.rename(columns={'COGS':'2019_COGS'})

sales_df.to_csv('C:/Users/xcxg109/NonDriveFiles/cats.csv')

for sup in suppliers:
    supplier_df = gcom.grainger_q(basic_hier_query, 'item.SUPPLIER_NO', sup)
    supplier_df = supplier_df.drop_duplicates()
    
    supplier_df = supplier_df.merge(parent_df, how= "inner", on=['Supplier_ID'])
    
    cats = supplier_df['Category_ID'].unique().tolist()

    print('Supplier ID {} -- {} of {}: {} categories'.format(sup, loop_count, len(suppliers), len(cats)))

    # get fill rate and SKU counts by category for supplier
    for cat in cats:
        loop_time = time.time()
        print('cat - ', cat)

        temp_df = supplier_df.loc[supplier_df['Category_ID']== cat]

        fill_sup = get_supplier_fill_rate(temp_df)
        temp_df = temp_df.merge(fill_sup, how= "outer", on=['Attr_ID'])

        sup_name = temp_df['Supplier_Name'].unique().tolist()
        sup_parent = temp_df['Supplier_Parent_Group'].unique().tolist()
        
        fill_category = get_category_fill_rate(cat)
        fill_category['Supplier_ID'] = sup
        fill_category['Supplier_Parent_Group'] = sup_parent[0]
        fill_category['Supplier_Name'] = sup_name[0]
        
        temp_df = temp_df.merge(fill_category, how= "outer", on=['Supplier_Parent_Group', 'Supplier_ID', 'Supplier_Name', \
                                        'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                                        'Category_Name', 'Attr_ID', 'Attribute_Name', 'Endeca_Ranking'])
        temp_df = pd.concat([fill_category, temp_df], sort=False)

        temp_df[['Supplier_Fill_Rate_%']] = temp_df[['Supplier_Fill_Rate_%']].fillna(value='0')        
        temp_df = temp_df.sort_values(by=['Category_ID', 'Attr_ID', 'Category_Fill_Rate_%', 'Supplier_Fill_Rate_%'])
            
        
#        temp_df.to_csv('C:/Users/xcxg109/NonDriveFiles/test.csv')
        
#        temp_df = temp_df.merge(fill_category, how= "outer", on=['Attribute_Name'])
#        temp_df['Count'] = 1
#        sku_count = pd.DataFrame(temp_df.groupby(['Attr_ID'])['Count'].sum())
#        sku_count = sku_count.rename(columns={'Count':'SKU_Count'})

#        temp_df = temp_df.merge(sku_count, how= "inner", on=['Attr_ID'])
#        temp_df = temp_df.merge(temp_fill, how= "inner", on=['Attr_ID', 'Fill_Rate_%'])

        temp_all_cats = pd.concat([temp_all_cats, temp_df], axis=0, sort=False) 
#        temp_df_fill = pd.concat([temp_df_fill, temp_fill], axis=0, sort=False)
        
    grainger_df = pd.concat([grainger_df, temp_all_cats], axis=0, sort=False) 
#    df_fill = pd.concat([df_fill, temp_df_fill], axis=0, sort=False)
#    grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/text.csv')
    loop_count += 1
    print("--- {} minutes ---".format(round((time.time() - loop_time)/60, 2)))

#grainger_df = grainger_df.drop('Grainger_Attribute_Value', axis='columns', inplace=True)
grainger_atts = grainger_df
grainger_df = grainger_df.merge(sales_df, how= "inner", on=['Grainger_SKU'])

data_out(grainger_df, grainger_atts)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))


#sku_count.to_csv('C:/Users/xcxg109/NonDriveFiles/SKUS.csv')
#grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/cogs.csv')