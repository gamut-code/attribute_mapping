# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import re
from grainger_query import GraingerQuery
from GWS_query import GWSQuery
from queries_WS import grainger_attr_query, grainger_value_query, ws_attr_values
import file_data_GWS as fd
import settings_NUMERIC as settings
import time

gcom = GraingerQuery()
gws = GWSQuery()

gws_attr_values="""
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
        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , tax_att.id as "WS_Attr_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att."dataType" as "Data_Type"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "WS_Attr_Value_ID"
--        , tax_att.description as "WS_Attribute_Definition"
        , tprodvalue.value as "WS_Original_Value"
        , tprodvalue.unit as "WS_Original_Unit"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        , tprodvalue."unitNormalized" as "Normalized_Unit"
        , tax_att."unitGroupId" as "Unit_Group_ID"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
        AND tprod.status = 3
        
    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"
        AND tax_att."multiValue" = 'false'

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"

    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"
        
    WHERE {} IN ({})
        """


def get_stats(df):
    """return unique values for each attribute with a count of how many times each is used in the node"""
    df['Count'] =1
    stats = pd.DataFrame(df.groupby(['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])['Count'].sum())
    return stats


def item_search(analysis, searchfor):
    """search the dictionary of attributes for any key containing the passed in value. Used to look for any 'Item' attributes"""
    total = [value for (key, value) in analysis.items() if searchfor in key]
    if len(total) > 1:
        total = max(total)
    return total


def get_fill_rate(df):
    browsable_skus = pd.DataFrame()

    # eliminate all discontinueds and R4/R9 before calculating fill rate
    browsable_skus = df
    pmCode = ['R4', 'R9']
    salesCode = ['DG', 'DV', 'WG', 'WV']
    browsable_skus = browsable_skus[~browsable_skus.PM_Code.isin(pmCode)]
    browsable_skus = browsable_skus[~browsable_skus.Sales_Status.isin(salesCode)]

    total = browsable_skus['Grainger_SKU'].nunique()

    if total > 0:
        browsable_skus = browsable_skus.drop_duplicates(subset=['Grainger_SKU', 'Grainger_Attribute_Name'])  #create list of unique grainger skus that feed into gamut query

        browsable_skus['Grainger_Fill_Rate_%'] = (browsable_skus.groupby('Grainger_Attribute_Name')['Grainger_Attribute_Name'].transform('count')/total)*100
        browsable_skus['Grainger_Fill_Rate_%'] = browsable_skus['Grainger_Fill_Rate_%'].map('{:,.2f}'.format)
    
        fill_rate = pd.DataFrame(browsable_skus.groupby(['Grainger_Attribute_Name'])['Grainger_Fill_Rate_%'].count()/total*100).reset_index()
        fill_rate = fill_rate.sort_values(by=['Grainger_Fill_Rate_%'], ascending=False)

        browsable_skus = browsable_skus[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate = fill_rate.merge(browsable_skus, how= "inner", on=['Grainger_Attribute_Name'])
        fill_rate['Grainger_Fill_Rate_%'] = fill_rate['Grainger_Fill_Rate_%'].map('{:,.2f}'.format)  

    else:
        df['Grainger_Fill_Rate_%'] = 'no browsable SKUs'
        fill_rate = df[['Grainger_Attribute_Name']].drop_duplicates(subset='Grainger_Attribute_Name')
        fill_rate['Grainger_Fill_Rate_%'] = 'no browsable SKUs'

    return fill_rate


def gws_values(df):
    df['WS_Value'] = ''
    
    for row in df.itertuples():
        dt = df.at[row.Index, 'Data_Type']
        val = df.at[row.Index, 'Normalized_Value']
        
        if dt == 'number':
            unit = df.at[row.Index, 'Normalized_Unit']
            
            ws_val = str(val) + ' ' + str(unit)
            df.at[row.Index, 'WS_Value'] = ws_val
            
        else:
            df.at[row.Index, 'WS_Value'] = val
                    
    return df

            
def compare_values(df):
    df['STEP-WS_Match?'] = ''
    
    for row in df.itertuples():
        gr_val = df.at[row.Index, 'Grainger_Attribute_Value']
        
        ws_val = df.at[row.Index, 'WS_Value']
        ws_val = str(ws_val)

    
        if ws_val == '' or ws_val == 'nan':
            orig_value = df.at[row.Index,'Grainger_Attribute_Value']
            orig_value = str(orig_value)

        if gr_val == ws_val:
            df.at[row.Index, 'STEP-WS_Match?'] = 'Y'            
        else:
            df.at[row.Index, 'STEP-WS_Match?'] = 'N'
            
    return df


def data_out(final_df, node, batch=''):
#    final_df = final_df.drop(final_df[(final_df['STEP-WS_Match?'] == 'Y' or final_df['Potential_Replaced_Values'] == '')])
#    final_df = final_df[final_df.Potential_Replaced_Values != '']
    final_df = final_df[final_df.Grainger_Attribute_Name != 'Item']
    
    final_df = final_df.sort_values(['Potential_Replaced_Values'], ascending=[True])
    
    final_df['concat'] = final_df['Grainger_Attribute_Name'].map(str) + final_df['Grainger_Attribute_Value'].map(str)
    final_df['Group_ID'] = final_df.groupby(final_df['concat']).grouper.group_info[0] + 1
    final_df = final_df[['Group_ID', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                'Category_Name', 'WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'PM_Code', \
                'Sales_Status', 'RELATIONSHIP_MANAGER_CODE', 'Grainger_SKU', 'WS_SKU', 'WS_Attr_ID', \
                'WS_Attr_Value_ID', 'WS_Attribute_Name', 'WS_Original_Value', 'Grainger_Attr_ID', \
                'Grainger_Attribute_Name', 'Grainger_Attribute_Value']]

    final_no_dupes = final_df.drop_duplicates(subset=['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])
    final_no_dupes = final_no_dupes [['Group_ID', 'Category_ID', 'Category_Name', 'Grainger_SKU', 'Grainger_Attr_ID', \
                                      'Grainger_Attribute_Name', 'Grainger_Attribute_Value']]
    final_no_dupes = final_no_dupes.rename(columns={'Grainger_SKU':'Example SKU'})

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_'+str(batch)+'_text_UOMs.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    final_no_dupes.to_excel (writer, sheet_name="Uniques", startrow=0, startcol=0, index=False)
    final_df.to_excel (writer, sheet_name="All Text UOMs", startrow=0, startcol=0, index=False)

    worksheet1 = writer.sheets['Uniques']
    worksheet2 = writer.sheets['All Text UOMs']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    col_widths = fd.get_col_widths(final_no_dupes)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    worksheet1.set_column('G:G', 50, layout)
    worksheet1.set_column('H:H', 30, layout)
    worksheet1.set_column('J:J', 50, layout)

    col_widths = fd.get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width)

    worksheet2.set_column('V:V', 50, layout)
    worksheet2.set_column('Y:Y', 50, layout)
    worksheet2.set_column('AA:AA', 50, layout)

    writer.save()


#determine SKU or node search
search_level = 'cat.CATEGORY_ID'
data_type = fd.values_search_type()


if data_type == 'grainger_query':
    search_level = fd.blue_search_level()
elif data_type == 'value' or data_type == 'name':
    while True:
        try:
            val_type = input('Search Type?:\n1. Exact value \n2. Value contained in field ')
            if val_type in ['1', 'EXACT', 'exact', 'Exact']:
                val_type = 'exact'
                break
            elif val_type in ['2', '%']:
                val_type = 'approx'
                break
        except ValueError:
            print('Invalid search type')
    
search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')
        
if data_type == 'grainger_query':
    gws_df = pd.DataFrame()
    
    for k in search_data:
        grainger_df = gcom.grainger_q(grainger_attr_query, search_level, k)
 
        if grainger_df.empty == False:
            df_stats = get_stats(grainger_df)
            df_fill = get_fill_rate(grainger_df)
            
            nodes = grainger_df['Category_ID'].unique()
            
            for n in nodes:
                gws_node = "'" + str(n) + "_DIV1'"
                print(gws_node)
 
                temp_df = gws.gws_q(gws_attr_values, 'pi_mappings.step_category_ids[1]', gws_node)
                gws_df = pd.concat([gws_df, temp_df], axis=0, sort=False) 
 
            gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].str.replace('_ATTR', '')
            gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].astype(int)
            
            gws_df = gws_values(gws_df)
            
            grainger_df = pd.merge(grainger_df, gws_df, how='left', left_on=['Grainger_SKU', 'Grainger_Attr_ID'], \
                                                                   right_on=['WS_SKU', 'STEP_Attr_ID'])
                               
            grainger_df = compare_values(grainger_df)
            
            grainger_df.dropna(subset=['Segment_ID'], inplace=True)
            
#            fd.attr_data_out(settings.directory_name, grainger_df, df_stats, df_fill, search_level)
            data_out(grainger_df, k)
            
        print (k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'yellow':
    for k in search_data:
        if isinstance(k, int):     #k.isdigit() == True:
            pass
        else:
            k = "'" + str(k) + "'"
        df = gcom.grainger_q(grainger_attr_query, 'yellow.PROD_CLASS_ID', k)
        if df.empty == False:
            df_stats = get_stats(df)
            df_fill = get_fill_rate(df)
            fd.attr_data_out(settings.directory_name, df, df_stats, df_fill, search_level, val_type)
        else:
            print('All SKUs are R4, R9, or discontinued')
       
        print (k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'sku':
        val_type = '_regular'
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)

        df = gcom.grainger_q(grainger_attr_query, 'item.MATERIAL_NO', sku_str)

        if df.empty == False:
            search_level = 'SKU'
            df_stats = get_stats(df)
            df_fill = get_fill_rate(df)
            fd.attr_data_out(settings.directory_name, df, df_stats, df_fill, search_level, val_type)

        else:
            print('All SKUs are R4, R9, or discontinued')

        print(search_data)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
        
elif data_type == 'name':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass

            else:
                k = "'" + str(k) + "'"

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"

        df = gcom.grainger_q(grainger_value_query, 'attr.DESCRIPTOR_NAME', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, search_level, 'ATTRIBUTES')

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

elif data_type == 'value':
    for k in search_data:
        if val_type == 'exact':
            if isinstance(k, int):  #k.isdigit() == True:
                pass
 
            else:
                k = "'" + str(k) + "'"

        elif val_type == 'approx':
            k = "'%" + str(k) + "%'"

        df = gcom.grainger_q(grainger_value_query, 'item_attr.ITEM_DESC_VALUE', k)

        if df.empty == False:
            fd.data_out(settings.directory_name, df, search_level, 'ATTRIBUTES')

        else:
            print('No results returned')

        print(k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
