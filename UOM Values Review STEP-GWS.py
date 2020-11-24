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
from queries_WS import grainger_attr_query, grainger_value_query, gws_attr_values
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
        , tax_att."numericDisplayType" as "Numeric_Display_Type"
        , tprodvalue."numeratorNormalized" as "Numerator"
        , tprodvalue."denominatorNormalized" as "Denominator"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.id as "Attribute_Value_ID"
--        , tax_att.description as "WS_Attribute_Definition"
        , tprodvalue.value as "Original_Value"
        , tprodvalue.unit as "Original_Unit"
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


def gws_values(df):
    df['WS_Value'] = ''
    
    for row in df.itertuples():
        dt = df.at[row.Index, 'Data_Type']
        val = df.at[row.Index, 'Normalized_Value']
        
        if dt == 'number':
            unit = df.at[row.Index, 'Normalized_Unit']
            display = df.at[row.Index, 'Numeric_Display_Type']
            
            if display == 'fraction':
                numer = df.at[row.Index, 'Numerator']
                denom = df.at[row.Index, 'Denominator']

                ws_val = str(numer) + '/' + str(denom) + ' ' + str(unit)
                df.at[row.Index, 'WS_Value'] = ws_val

            elif display == 'decimal':
                # add thousands comma separator to value                
                try:
                    val = int(val)
                except:
                    val = float(val)
                    
                val = '{:,}'.format(val)
                print('val = ', val)

                ws_val = str(val) + ' ' + str(unit)
                df.at[row.Index, 'WS_Value'] = ws_val
            
            else:
                print('display type = ', display)

        else:
            df.at[row.Index, 'WS_Value'] = val
                    
    return df


def process_vals(df, orig_value):
    """ clean up the sample values column """
    search_string = ''
    pot_value = orig_value

    if '"' in pot_value:
        search_string = search_string+'; '+'"'
        pot_value = pot_value.replace('"', ' in')

    if 'min.' in pot_value:
        search_string = search_string+'; '+'min.'
        pot_value = pot_value.replace('min.', 'min')
        
    if 'in.' in pot_value or 'In.' in pot_value:
        search_string = search_string+'; '+'in.'
        pot_value = pot_value.replace('in.', ' in')
        pot_value = pot_value.replace('In.', ' in')

    if 'ft.' in pot_value or 'Ft.' in pot_value:
        search_string = search_string+'; '+'ft.'
        pot_value = pot_value.replace('ft.', 'ft')

    if 'yd.' in pot_value:
        search_string = search_string+'; '+'yd.'
        pot_value = pot_value.replace('yd.', 'yd')

    if 'fl.' in pot_value:
        search_string = search_string+'; '+'fl.'
        pot_value = pot_value.replace('fl.', 'fl')

    if 'oz.' in pot_value:
        search_string = search_string+'; '+'oz.'
        pot_value = pot_value.replace('oz.', 'oz')

    if 'pt.' in pot_value:
        search_string = search_string+'; '+'pt.'
        pot_value = pot_value.replace('pt.', 'pt')

    if 'qt.' in pot_value:
        search_string = search_string+'; '+'qt.'
        pot_value = pot_value.replace('qt.', 'qt')

    if 'kg.' in pot_value:
        search_string = search_string+'; '+'kg.'
        pot_value = pot_value.replace('kg.', 'kg')

    if 'gal.' in pot_value:
        search_string = search_string+'; '+'gal.'
        pot_value = pot_value.replace('gal.', 'gal')

    if 'lb.' in pot_value:
        search_string = search_string+'; '+'lb.'
        pot_value = pot_value.replace('lb.', 'lb')

    if 'cu.' in pot_value:
        search_string = search_string+'; '+'cu.'
        pot_value = pot_value.replace('cu.', 'cu')
    
    if 'cf.' in pot_value:
        search_string = search_string+'; '+'cf.'
        pot_value = pot_value.replace('cf.', 'cu ft')

    if 'sq.' in pot_value:
        search_string = search_string+'; '+'sq.'
        pot_value = pot_value.replace('sq.', 'sq')
    
    if '° C' in pot_value or '°C' in pot_value:
        search_string = search_string+'; '+'° C'
        pot_value = pot_value.replace('° C', '°C')
    
    if '° F' in pot_value or '°F' in pot_value:
        search_string = search_string+'; '+'° F'
        pot_value = pot_value.replace('° F', '°F')

    if 'deg.' in pot_value:
        search_string = search_string+'; '+'deg.'
        pot_value = pot_value.replace('deg.', '°')
    
    if 'ga.' in pot_value:
        search_string = search_string+'; '+'ga.'
        pot_value = pot_value.replace('ga.', 'ga')

    if 'sec.' in pot_value:
        search_string = search_string+'; '+'sec.'
        pot_value = pot_value.replace('sec.', 'sec')

    if 'hr.' in pot_value:
        search_string = search_string+'; '+'hr.'
        pot_value = pot_value.replace('hr.', 'hr')

    if 'wk.' in pot_value:
        search_string = search_string+'; '+'wk.'
        pot_value = pot_value.replace('wk.', 'wk')

    if 'mo.' in pot_value:
        search_string = search_string+'; '+'mo.'
        pot_value = pot_value.replace('mo.', 'mo')

    if 'yr.' in pot_value:
        search_string = search_string+'; '+'yr.'
        pot_value = pot_value.replace('yr.', 'yr')

    if 'µ' in pot_value:
        search_string = search_string+'; '+'µ'
        pot_value = pot_value.replace('µ', 'u')

    if 'dia.' in pot_value or 'Dia.' in pot_value:
        search_string = search_string+'; '+'dia.'
        pot_value = pot_value.replace('dia.', 'dia')
        pot_value = pot_value.replace('Dia.', 'dia')
        
    if 'and' in [pot_value]:
        search_string = search_string+'; '+'and'
        pot_value = pot_value.replace('and', '&')

    if 'bu.' in pot_value:
        search_string = search_string+'; '+'bu.'
        pot_value = pot_value.replace('bu.', 'bu')

    if 'cal.' in pot_value:
        search_string = search_string+'; '+'cal.'
        pot_value = pot_value.replace('cal.', 'cal')

    if 'dim.' in pot_value:
        search_string = search_string+'; '+'dim.'
        pot_value = pot_value.replace('dim.', 'dimensions')

    if 'doz.' in pot_value:
        search_string = search_string+'; '+'doz.'
        pot_value = pot_value.replace('doz.', 'doz')

    if 'gn.' in pot_value:
        search_string = search_string+'; '+'gn.'
        pot_value = pot_value.replace('gn.', 'gn')

    if 'gr.' in pot_value:
        search_string = search_string+'; '+'gr.'
        pot_value = pot_value.replace('gr.', 'gr')

    if 'wt.' in pot_value:
        search_string = search_string+'; '+'wt.'
        pot_value = pot_value.replace('wt.', 'wt')
        
    if 'Hi-Vis' in [pot_value] or 'Hi Vis' in [pot_value] or 'Hi-Viz' in [pot_value] \
        or 'Hi Viz' in [pot_value] or 'Hi Visibility' in [pot_value]:
        search_string = search_string+'; '+'Hi-Vis'
        pot_value = pot_value.replace('Hi-Vis', 'Hi-Visibility')
        
    if 'I.D.' in pot_value:
        search_string = search_string+'; '+'I.D.'
        pot_value = pot_value.replace('I.D.', 'ID')

    if 'ips' in [pot_value]:
        search_string = search_string+'; '+'ips'
        pot_value = pot_value.replace('ips', 'in/sec')

    if 'max.' in pot_value:
        search_string = search_string+'; '+'max.'
        pot_value = pot_value.replace('max.', 'max')

    if 'mi.' in pot_value:
        search_string = search_string+'; '+'mi.'
        pot_value = pot_value.replace('mi.', 'mi')

    if 'mmHg' in pot_value:
        search_string = search_string+'; '+'mmHg'
        pot_value = pot_value.replace('mmHg', 'mm Hg')

    if 'O.D.' in pot_value:
        search_string = search_string+'; '+'O.D.'
        pot_value = pot_value.replace('O.D.', 'OD')

    if 'OD' in [pot_value]:
        search_string = search_string+'; '+'OD'
        pot_value = pot_value.replace('OD', 'Op Den')

#        if '1-Phase' in pot_value:
#            search_string = search_string+'; '+'1-Phase'
#            pot_value = pot_value.replace('1-Phase', 'single-phase')

#        if '3-Phase' in pot_value:
#            search_string = search_string+'; '+'3-Phase'
#            pot_value = pot_value.replace('3-Phase', 'three-phase')

    if 'pcs.' in pot_value:
        search_string = search_string+'; '+'pcs.'
        pot_value = pot_value.replace('pcs.', 'pieces')

    if 'pk.' in pot_value:
        search_string = search_string+'; '+'pk.'
        pot_value = pot_value.replace('pk.', 'pk')

    if 'pr.' in pot_value:
        search_string = search_string+'; '+'pr.'
        pot_value = pot_value.replace('pr.', 'pr')

    if 'qty.' in pot_value:
        search_string = search_string+'; '+'qty.'
        pot_value = pot_value.replace('qty.', 'qty')

    if 'S.P.' in pot_value:
        search_string = search_string+'; '+'S.P.'
        pot_value = pot_value.replace('S.P.', 'SP')

    if 'Sh. Wt.' in pot_value:
        search_string = search_string+'; '+'Sh. Wt.'
        pot_value = pot_value.replace('Sh. Wt.', 'shipping weight')
        
    if 'SS' in [pot_value]:
        search_string = search_string+'; '+'SS'
        pot_value = pot_value.replace('SS', 'stainless steel')
  
    if 't' in [pot_value]:
        search_string = search_string+'; '+'t'
        pot_value = pot_value.replace('t', 'metric ton')

    if 'VAC' in pot_value:
        if 'HVAC' not in pot_value:
            search_string = search_string+'; '+'VAC'
            pot_value = pot_value.replace('VAC', 'V AC')

    if 'VDC' in pot_value:
        search_string = search_string+'; '+'VDC'
        pot_value = pot_value.replace('VDC', 'V DC')

    if 'vol.' in pot_value:
        search_string = search_string+'; '+'vol.'
        pot_value = pot_value.replace('vol.', 'vol')

    if 'XXXXL' in pot_value:
        search_string = search_string+'; '+'XXXXL'
        pot_value = pot_value.replace('XXXXL', '4XL')

    if 'XXXL' in pot_value:
        search_string = search_string+'; '+'XXXL'
        pot_value = pot_value.replace('XXXL', '3XL')

    if 'XXL' in pot_value:
        search_string = search_string+'; '+'XXL'
        pot_value = pot_value.replace('XXL', '2XL')

    if 'CFM' in pot_value:
        search_string = search_string+'; '+'CFM'
        pot_value = pot_value.replace('CFM', 'cfm')

    if 'LFM' in pot_value:
        search_string = search_string+'; '+'LFM'
        pot_value = pot_value.replace('LFM', 'lfm')

    if "''" in pot_value:
        search_string = search_string+'; '+"''"
        pot_value = pot_value.replace("''", 'in')

    if 'in x' in pot_value:
        pot_value = pot_value.replace('in x', 'in x ')

    if 'in )' in pot_value:
        pot_value = pot_value.replace('in )', 'in)')
        
    if '  ' in pot_value:
        pot_value = pot_value.replace('  ', ' ')

    r = re.compile('^\d*[\.\/]?\d*')

    split_num, split_text = re.split(r, pot_value)

    if split_text == 'K':
        search_string = search_string+'; '+'K'
        pot_value = pot_value.replace('K', ' K')

    if split_text == 'HP':
        search_string = search_string+'; '+'HP'
        pot_value = pot_value.replace('HP', '')

    search_string = search_string[2:]
#    pot_value = pot_value.strip()
    pot_value = split_text

    return df, search_string, pot_value

            
def compare_values(df):
    df['STEP-WS_Match?'] = ''
    df['Potential_Replaced_Values'] = ''
    df['Revised_Value'] = ''
    
    for row in df.itertuples():
        gr_val = df.at[row.Index, 'Grainger_Attribute_Value']
        
        ws_val = df.at[row.Index, 'WS_Value']
        ws_val = str(ws_val)

    
        if ws_val == '' or ws_val == 'nan':
            orig_value = df.at[row.Index,'Grainger_Attribute_Value']
            orig_value = str(orig_value)

            df, search_string, pot_value = process_vals(df, orig_value)

            if search_string == '' or search_string == 'nan':
                pass   # do nothing -- don't populate 'Revised Values' if no changes needed
            else:
                df.at[row.Index,'Potential_Replaced_Values'] = search_string
                df.at[row.Index,'Revised_Value'] = pot_value

        else:
            if gr_val == ws_val:
                df.at[row.Index, 'STEP-WS_Match?'] = 'Y'
            
            else:
                df.at[row.Index, 'STEP-WS_Match?'] = 'N'

                orig_value = df.at[row.Index,'Grainger_Attribute_Value']
                orig_value = str(orig_value)

                df, search_string, pot_value = process_vals(df, orig_value)

                print('search string = ', search_string)
                if search_string == '' or search_string == 'nan':
                    pass   # do nothing -- don't populate 'Revised Values' if no changes needed
                else:
                    print('pot_value = ', pot_value)
                    df.at[row.Index,'Potential_Replaced_Values'] = search_string
                    df.at[row.Index,'Revised_Value'] = pot_value
            
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
                'Numeric_Display_Type', 'WS_Attribute_Name', 'Grainger_Attr_ID', 'Grainger_Attribute_Name', \
                'Grainger_Attribute_Value', 'Potential_Replaced_Values', 'Revised_Value']]

    final_no_dupes = final_df.drop_duplicates(subset=['Grainger_Attribute_Name', 'Grainger_Attribute_Value'])
    final_no_dupes = final_no_dupes [['Group_ID', 'Category_ID', 'Category_Name', 'Grainger_SKU', 'Grainger_Attr_ID', \
                                      'Grainger_Attribute_Name', 'Grainger_Attribute_Value', \
                                      'Potential_Replaced_Values', 'Revised_Value']]
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
                        
            data_out(grainger_df, k)
            
        print (k)
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))