# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 20:34:02 2020

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
import WS_query_code as q
import WS_file_data as fd
import settings_NUMERIC as settings
import time

ws_basic_query="""
    SELECT
          tprod."gtPartNumber" as "WS_SKU"
        , tprod."categoryId" AS "GWS_Node_ID"
        , supplier."supplierNo" as "Supplier_ID"
        
    FROM taxonomy_product tprod

    INNER JOIN supplier_product supplier
        ON supplier.id = tprod."supplierProductId"
        
    WHERE {} IN ({})
"""


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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , supplier."supplierNo" as "Supplier_ID"
        , tprod.supplier as "Supplier_Name"
        , tax_att.id as "WS_Attr_ID"
--        , tprodvalue.id as "WS_Attr_Value_ID"
--        , tax_att."multiValue" as "Multivalue"
--        , tax_att."dataType" as "Data_Type"
--  	    , tax_att."numericDisplayType" as "Numeric_Display_Type"
--        , tax_att.description as "WS_Attribute_Definition"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
--        , tprodvalue.unit as "Original_Unit"
--        , tprodvalue."valueNormalized" as "Normalized_Value"
--        , tprodvalue."unitNormalized" as "Normalized_Unit"
--	    , tprodvalue."numeratorNormalized" as "Numerator"
--	    , tprodvalue."denominatorNormalized" as "Denominator"
--        , tax_att."unitGroupId" as "Unit_Group_ID"
        , tax_att.rank as "Rank"
        , tax_att.priority as "Priority"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
        AND tprod.status = 3
        
    FULL OUTER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att.deleted = 'false'

    FULL OUTER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        AND tprodvalue.deleted = 'false'
        
    FULL OUTER JOIN supplier_product supplier
        ON supplier.id = tprod."supplierProductId"
        
    WHERE {} IN ({})
        """
        

pd.options.mode.chained_assignment = None

gws = GWSQuery()


def get_category_fill_rate(cat_df):
    browsable_skus = cat_df

    browsable_skus['Original_Value'].replace('', np.nan, inplace=True)

    #calculate fill rates at the attribute / category level    
    total = browsable_skus['WS_SKU'].nunique()
    print ('cat total = ', total)
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['WS_SKU', 'WS_Attr_ID'])
    browsable_skus.dropna(subset=['Original_Value'], inplace=True)
    
#    browsable_skus['Category_Fill_Rate_%'] = (browsable_skus.groupby('WS_Attr_ID')['WS_Attr_ID'].transform('count')/total)*100
#    browsable_skus['Category_Fill_Rate_%'] = browsable_skus['Category_Fill_Rate_%'].map('{:,.2f}'.format)

    browsable_skus['Category_Fill_Rate_%'] = (browsable_skus.groupby('WS_Attr_ID')['WS_Attr_ID'].apply(lambda x: x.notnull().mean))
    browsable_skus['Category_Fill_Rate_%'] = browsable_skus['Category_Fill_Rate_%'].map('{:,.2f}'.format)
     
    fill_rate_cat = pd.DataFrame(browsable_skus.groupby(['WS_Attr_ID'])['Category_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', \
                                     'Supplier_ID', 'Supplier_Name', 'WS_Attr_ID', 'WS_Attribute_Name', \
                                     'Priority', 'Rank']]
    browsable_skus = browsable_skus.drop_duplicates(subset='WS_Attr_ID')
    
    fill_rate_cat = fill_rate_cat.merge(browsable_skus, how= "inner", on=['WS_Attr_ID'])
    fill_rate_cat['Category_Fill_Rate_%'] = fill_rate_cat['Category_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate_cat = fill_rate_cat.merge(browsable_skus, how= "inner", on=['WS_Category_ID', \
                                                        'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', \
                                                        'Supplier_ID', 'Supplier_Name', 'WS_Attr_ID', \
                                                        'WS_Attribute_Name', 'Priority', 'Rank'])

    return fill_rate_cat
    
    
def get_supplier_fill_rate(df):
    # note: df here is already segregated by category_ID
    browsable_skus = df
    
    #calculate fill rates at the attribute / category level    
    total = browsable_skus['WS_SKU'].nunique()
    print ('sup total = ', total)
    
    browsable_skus = browsable_skus.drop_duplicates(subset=['WS_SKU', 'WS_Attr_ID'])
    browsable_skus.dropna(subset=['Original_Value'], inplace=True)
    
    browsable_skus['Category_Supplier_Fill_Rate_%'] = (browsable_skus.groupby('WS_Attr_ID')['WS_Attr_ID'].apply(lambda x: x.notnull().mean))
    browsable_skus['Category_Supplier_Fill_Rate_%'] = browsable_skus['Category_Supplier_Fill_Rate_%'].map('{:,.2f}'.format)

    browsable_skus['Batch_Supplier_Fill_Rate_%'] = (browsable_skus.groupby('WS_Attr_ID')['WS_Attr_ID'].apply(lambda x: x.notnull().mean))
    browsable_skus['Batch_Supplier_Fill_Rate_%'] = browsable_skus['Batch_Supplier_Fill_Rate_%'].map('{:,.2f}'.format)
    
    fill_rate_sup = pd.DataFrame(browsable_skus.groupby(['WS_Attr_ID'])['Category_Supplier_Fill_Rate_%'].count()/total*100).reset_index()

    browsable_skus = browsable_skus[['WS_Attr_ID']].drop_duplicates(subset='WS_Attr_ID')
    
    fill_rate_sup = fill_rate_sup.merge(browsable_skus, how= "inner", on=['WS_Attr_ID'])
    fill_rate_sup['Category_Supplier_Fill_Rate_%'] = fill_rate_sup['Category_Supplier_Fill_Rate_%'].map('{:,.2f}'.format)  

    fill_rate_sup = fill_rate_sup.merge(browsable_skus, how= "inner", on=['WS_Attr_ID'])

    fill_rate_sup.to_csv('C:/Users/xcxg109/NonDriveFiles/browse.csv')

    return fill_rate_sup


def data_out(df, atts_df, batch=''):
    # output for sku-based pivot table
    fill = atts_df[['Supplier_ID', 'Supplier_Name', 'WS_Category_ID', 'WS_Category_Name', \
               'WS_Node_ID', 'WS_Node_Name', 'WS_Attr_ID', 'WS_Attribute_Name', \
               'Priority', 'Rank', 'Supplier_Fill_Rate_%', 'Category_Fill_Rate_%']] 
    fill = fill.drop_duplicates(subset=['WS_Node_ID', 'WS_Attr_ID'])
    fill = fill.sort_values(by=['WS_Category_Name', 'WS_Node_Name', 'Rank'])

    fill[['Category_Fill_Rate_%']] = fill[['Category_Fill_Rate_%']].fillna(value='0')        
        
    outfile = 'C:/Users/xcxg109/NonDriveFiles/SUPPLIER_REPORT_'+str(batch)+'_.xlsx'

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    fill.to_excel(writer, sheet_name='Attribute Fill Rates', startrow =0, startcol=0, index=False)

    worksheet = writer.sheets['Attribute Fill Rates']

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
        worksheet.set_column(i, i, width)

    writer.save()



init_time = time.time()

ws_df = pd.DataFrame()
supplier_df = pd.DataFrame()
category_df = pd.DataFrame()
temp_all_cats = pd.DataFrame()
temp_all_atts = pd.DataFrame()
full_df = pd.DataFrame()
full_atts = pd.DataFrame()

print('working....')

#request the type of data to pull: blue or yellow, SKUs or node, single entry or read from file
data_type = fd.WS_search_type()
search_level = 'tprod."categoryId"'

#ask user for node number/SKU or pull from file if desired    
search_data = fd.data_in(data_type, settings.directory_name)


if data_type == 'sku':
    search_level = 'SKU'

    print('batch = {} SKUs'.format(len(search_data)))
    if len(search_data)>4000:
        num_lists = round(len(search_data)/4000, 0)
        num_lists = int(num_lists)

        if num_lists == 1:
            num_lists = 2

        print('running GWS SKUs in {} batches'.format(num_lists))

        size = round(len(search_data)/num_lists, 0)
        size = int(size)

        div_lists = [search_data[i * size:(i + 1) * size] for i in range((len(search_data) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            print('batch {} of {}'.format(k+1, num_lists))
            sku_str  = ", ".join("'" + str(i) + "'" for i in div_lists[k])

            temp_df = gws.gws_q(ws_basic_query, 'tprod."gtPartNumber"', sku_str)
            ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False)

    else:
        sku_str  = ", ".join("'" + str(i) + "'" for i in search_data)
        
        ws_df = gws.gws_q(ws_basic_query, 'tprod."gtPartNumber"', sku_str)

    if ws_df.empty == False:
        # pull all L3s for the supplier and get attribute data on each node
        suppliers = ws_df['Supplier_ID'].unique().tolist()
        print('# suppliers = ', len(suppliers))
        loop_count = 1
        
        for sup in suppliers:
            start_time = time.time()

            # pull all nodes for the supplier and get attribute data on each node            
            sup_df = ws_df.loc[ws_df['Supplier_ID'] == sup]
            cats = sup_df['GWS_Node_ID'].unique().tolist()
            
            print('Supplier {} -- {} of {}: {} cat'.format(sup, loop_count, len(suppliers), len(cats)))

            # get fill rate and SKU counts by category for supplier
            for cat in cats:
                print('cat - ', cat)

                # temp_df is the ENTIRE category, supplier_df is filtered by supplier ID        
                temp_df = gws.gws_q(ws_attr_values, 'tprod."categoryId"', cat)

                # get category fill rates first because supplier may not have all active attributes in a category
                fill_category = get_category_fill_rate(temp_df)

                # create filtered supplier_df and get fill rates specific to supplier
                supplier_df = temp_df.loc[temp_df['Supplier_ID']== sup]
                supplier_df.to_csv('C:/Users/xcxg109/NonDriveFiles/sup_df.csv')
                fill_supplier = get_supplier_fill_rate(supplier_df)

                # set up the fill_supplier rows for a merge with temp_df -- this ensures that attributes active in the node 
                # but not populated by the supplier are still included in the the df
                sup_name = supplier_df['Supplier_Name'].unique().tolist()

                fill_category['Supplier_ID'] = sup
                fill_category['Supplier_Name'] = sup_name[0]        

                temp_df = temp_df.merge(fill_category, how="outer", on=['WS_Category_ID', \
                                        'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'Supplier_ID', \
                                        'Supplier_Name', 'WS_Attr_ID', 'WS_Attribute_Name', 'Priority', \
                                        'Rank']) 
    
                temp_df = temp_df.merge(fill_supplier, how="outer", on=['WS_Attr_ID'])
                temp_df[['Supplier_Fill_Rate_%']] = temp_df[['Supplier_Fill_Rate_%']].fillna(value='0')        

                temp_df = temp_df.sort_values(by=['WS_Category_ID', 'WS_Attr_ID', 'Category_Fill_Rate_%', \
                                                  'Supplier_Fill_Rate_%'])
                temp_df = temp_df.drop_duplicates('WS_Attr_ID', keep='first')

                temp_all_cats = pd.concat([temp_all_cats, supplier_df], axis=0, sort=False) 
                temp_all_atts = pd.concat([temp_all_atts, temp_df], axis=0, sort=False)

            full_df = pd.concat([full_df, temp_all_cats], axis=0, sort=False) 
            full_atts = pd.concat([full_atts, temp_all_atts], axis=0, sort=False) 

            print("--- segment: {} minutes ---".format(round((time.time() - start_time)/60, 2)))
            loop_count += 1
        
if len(full_df) > 300000:
    count = 1

    # split into multiple dfs of 40K rows, creating at least 2
    num_lists = round(len(full_df)/300000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(full_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))
        
        data_out(object, full_atts, count)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
    data_out(full_df, full_atts)

print("--- total time: {} minutes ---".format(round((time.time() - init_time)/60, 2)))

#temp_df.to_csv('C:/Users/xcxg109/NonDriveFiles/temp_df.csv')