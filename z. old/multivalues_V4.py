# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 09:28:25 2020

GWS multivalue report 

@author: xcxg109
"""

import pandas as pd
import numpy as np
from GWS_query import GWSQuery
import file_data_GWS as fd
import time
import settings_NUMERIC as settings
from grainger_query import GraingerQuery
import WS_query_code as q

pd.options.mode.chained_assignment = None

gws = GWSQuery()
gcom = GraingerQuery()


gws_values_single="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "Gamut_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tax_att."dataType" as "Data_Type"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."multiValue" = 'TRUE'
        AND {} IN ({})
        """

gws_values_group="""
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "WS_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tax_att."dataType" as "Data_Type"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
        
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."multiValue" = 'TRUE'
        """


def get_col_widths(df):
    # find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def concat_values(df):
    # build string of all values for stats front page
    temp_final = pd.DataFrame()
    all_vals = pd.DataFrame()
    
    func_df = df.copy()
    func_df['Count'] =1

    atts = func_df['WS_Attribute_Name'].unique().tolist()
    
    vals = pd.DataFrame(func_df.groupby(['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'Original_Value'])['Count'].sum())
    vals = vals.reset_index()

    for attribute in atts:
        temp_att_df = vals.loc[vals['WS_Attribute_Name']== attribute]
        temp_att_df['List_of_Values'] = ''

        skus = temp_att_df['WS_SKU'].unique().tolist()

        for sku in skus:
            temp_sku_df = temp_att_df.loc[temp_att_df['WS_SKU']== sku]
            
            if len(temp_sku_df.index) > 1:
                temp_sku_df['List_of_Values'] = '; '.join(item for item in temp_sku_df['Original_Value'])
                temp_sku_df['#_Values'] = len(temp_sku_df.index)
                
                temp_final = pd.concat([temp_final, temp_sku_df], axis=0)
                
        all_vals = pd.concat([all_vals, temp_final], axis=0)

    if all_vals.empty == False:
        all_vals = all_vals[['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'List_of_Values', '#_Values']]
        all_vals = all_vals.drop_duplicates(subset=['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU'])

    return all_vals


def process_vals(df):
    """ clean up the sample values column """
        
    for row in df.itertuples():
        search_string = ''

        orig_value = df.at[row.Index,'Grainger_Attribute_Value']
        orig_value = str(orig_value)

        pot_value = orig_value

        if '"' in pot_value:
            search_string = search_string+'; '+'"'
            pot_value = pot_value.replace('"', ' in ')

        if 'min.' in pot_value:
            search_string = search_string+'; '+'min.'
            pot_value = pot_value.replace('min.', 'min')
            
        if 'in.' in pot_value or 'In.' in pot_value:
            search_string = search_string+'; '+'in.'
            pot_value = pot_value.replace('in.', ' in ')

        if 'ft.' in pot_value:
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
# revised LOV search for these?
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

        if '1-Phase' in pot_value:
            search_string = search_string+'; '+'1-Phase'
            pot_value = pot_value.replace('1-Phase', 'single-phase')

        if '3-Phase' in pot_value:
            search_string = search_string+'; '+'3-Phase'
            pot_value = pot_value.replace('3-Phase', 'three-phase')

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

        if 'degrees' in pot_value or 'Degrees' in pot_value:
            search_string = search_string+'; '+'degrees'
            pot_value = pot_value.replace('degrees', '°')
            pot_value = pot_value.replace('Degrees', '°')

        if "''" in pot_value:
            search_string = search_string+'; '+"''"
            pot_value = pot_value.replace("''", 'in')

        if 'in x' in pot_value:
            pot_value = pot_value.replace('in x', 'in x ')

        if 'in )' in pot_value:
            pot_value = pot_value.replace('in )', 'in)')
            
        if ' °' in pot_value:
            pot_value = pot_value.replace(' °', '°')

        if '  ' in pot_value:
            pot_value = pot_value.replace('  ', ' ')
            
        search_string = search_string[2:]

        df.at[row.Index,'Potential_Replaced_Values'] = search_string
        df.at[row.Index,'Revised Value'] = pot_value
    
    return df    


def process_nodes(grainger_all, df, node):

    process_ws_df = pd.DataFrame()
    values = pd.DataFrame()

    temp_df = df.loc[df['WS_Node_Name']== node]
    temp_df['Count'] =1

    temp_grainger = grainger_all.loc[grainger_all['STEP_Category_Name']==node]

    if temp_grainger.empty==False:
        print(temp_grainger['STEP_Category_Name'].unique())
        temp_grainger = temp_grainger[['WS_SKU', 'WS_Attribute_Name', 'Grainger_Attribute_Value']]
                               
        cols = ['WS_SKU', 'WS_Attribute_Name']
        temp_df = temp_df.join(temp_grainger.set_index(cols), on=cols)

        process_ws_df = pd.concat([process_ws_df, temp_df], axis=0, sort=False)

        temp_df = concat_values(temp_df)                
        values = pd.concat([values, temp_df], axis=0)

    else:                
        print('WS node = ', node)
        ws_skus = temp_df['WS_SKU'].unique().tolist()            
        print('{} : {} SKUs'.format(node, len(ws_skus)))
        
        if len(ws_skus)>0:
            for sku in ws_skus:
                temp_ws = df.loc[df['WS_SKU']== sku]
                temp_grainger = grainger_all.loc[grainger_all['WS_SKU']== sku]
                print('found {} in Grainger node {}'.format(sku, temp_grainger['STEP_Category_Name'].unique()))
                temp_grainger = temp_grainger[['WS_SKU', 'WS_Attribute_Name', 'Grainger_Attribute_Value']]
            
                cols = ['WS_SKU', 'WS_Attribute_Name']
                temp_ws = temp_ws.join(temp_grainger.set_index(cols), on=cols)
        
                process_ws_df = pd.concat([process_ws_df, temp_ws], axis=0, sort=False)  
                        
            temp_ws = concat_values(temp_ws)                
            values = pd.concat([values, temp_ws], axis=0)

    process_ws_df = process_vals(process_ws_df)

#    process_ws_df.to_csv('C:/Users/xcxg109/NonDriveFiles/process.csv')
#    values.to_csv('C:/Users/xcxg109/NonDriveFiles/values.csv')

    return process_ws_df, values

    
def data_out(df, node):
    df = df.sort_values(['WS_Node_Name', 'WS_Attribute_Name', 'WS_SKU', 'Original_Value'], \
                            ascending=[True, True, True, True])
    
    df_upload = df[['WS_Node_ID', 'WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', 'List_of_Values',\
                    '#_Values', 'Grainger_Attribute_Value', 'Potential_Replaced_Values', 'Revised Value']]
    df_upload['Count'] = 1
    df_upload = pd.DataFrame(df_upload.groupby(['WS_Node_ID', 'WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', \
                                                'List_of_Values', '#_Values', 'Grainger_Attribute_Value', \
                                                'Potential_Replaced_Values', 'Revised Value'])['Count'].sum())
    df_upload = df_upload.reset_index()
    df_upload = df_upload.drop(['Count'], axis=1)

    df_upload['concat'] = df_upload['WS_Attribute_Name'].map(str) + df_upload['List_of_Values'].map(str)
    df_upload['Group_ID'] = df_upload.groupby(df_upload['concat']).grouper.group_info[0] + 1
    df_upload = df_upload.drop(['concat'], axis=1)

    df_upload = df_upload[['Group_ID', 'WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name', \
                           'List_of_Values', '#_Values', 'Grainger_Attribute_Value', 'Potential_Replaced_Values', \
                           'Revised Value']]
                                            
    df_no_dupes = df_upload.drop_duplicates(subset=['WS_Attribute_Name', 'List_of_Values'])
            
    df_no_dupes = df_no_dupes.rename(columns={'WS_SKU':'Example SKU'})

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_multivalues.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    df_no_dupes.to_excel (writer, sheet_name="Unique Values", startrow=0, startcol=0, index=False)
    df_upload.to_excel (writer, sheet_name="Upload Sheet", startrow=0, startcol=0, index=False)

    worksheet1 = writer.sheets['Unique Values']
    worksheet2 = writer.sheets['Upload Sheet']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    col_widths = get_col_widths(df_no_dupes)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    worksheet1.set_column('G:G', 70, layout)
    worksheet1.set_column('I:I', 70, layout)
    worksheet1.set_column('K:K', 70, layout)

    col_widths = get_col_widths(df_upload)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
       if width > 40:
           width = 40
       elif width < 10:
           width = 10
       worksheet2.set_column(i, i, width)
    
       worksheet2.set_column('G:G', 70, layout)
       worksheet2.set_column('I:I', 70, layout)
       worksheet2.set_column('K:K', 70, layout)

       writer.save()


ws_df = pd.DataFrame()
ws_df_loop = pd.DataFrame()
att_vals = pd.DataFrame()
att_vals_loop = pd.DataFrame()
init_ws_df  = pd.DataFrame()

data_type = 'gws_query'

while True:
    try:
        search_level = input("Search by: \n1. Node Group \n2. Single Category \n3. SKU ")
        if search_level in ['1', 'g', 'G']:
            search_level = 'group'
            break
        elif search_level in ['2', 's', 'S']:
            search_level = 'single'
            break
        elif search_level in ['3', 'sku', 'SKU']:
            data_type = 'sku'
            break
    except ValueError:
        print('Invalid search type')
        
search_data = fd.data_in(data_type, settings.directory_name)

print('working...')
start_time = time.time()

# read in grainger data
allCATS_df = q.get_att_values()            

if data_type == 'gws_query':
    for node in search_data:
        start_time = time.time()

        if search_level == 'single':
            init_ws_df = gws.gws_q(gws_values_single, 'tprod."categoryId"', node)

        elif search_level == 'group':
            init_ws_df = gws.gws_q(gws_values_group, 'tprod."categoryId"', node)

        print('k = ', node)
    
        if init_ws_df.empty == False:
            node_names = init_ws_df['WS_Node_Name'].unique().tolist()
            print('number of nodes = ', len(node_names))

            if len(node_names)>100:
                num_lists = round(len(node_names)/100, 0)
                num_lists = int(num_lists)
    
                if num_lists == 1:
                    num_lists = 2
                print('running WS Nodes in {} batches'.format(num_lists))

                size = round(len(node_names)/num_lists, 0)
                size = int(size)

                div_lists = [node_names[i * size:(i + 1) * size] for i in range((len(node_names) + size - 1) // size)]

                for k in range(0, len(div_lists)):
                    print('\n\nBATCH ', k+1)
                    
                    for j in node_names:
                        temp_node_df, temp_att_vals = process_nodes(allCATS_df, init_ws_df, j)
                        ws_df_loop = pd.concat([ws_df_loop, temp_node_df], axis=0, sort=False)
                        att_vals_loop = pd.concat([att_vals_loop, temp_att_vals], axis=0, sort=False)

                    ws_df = pd.concat([ws_df, ws_df_loop], axis=0, sort=False)
                    att_vals = pd.concat([att_vals, att_vals_loop], axis=0, sort=False)
                            
                if att_vals.empty==False:            
                    ws_df = pd.merge(ws_df, att_vals, on=['WS_Node_Name', 'WS_SKU', 'WS_Attribute_Name'])

                if 'List_of_Values' in ws_df.columns:
                    data_out(ws_df, node)
                    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    

            else:
                for n in node_names:
                   ws_df = process_nodes(allCATS_df, init_ws_df, n)

                if att_vals.empty==False:            
                    ws_df = pd.merge(ws_df, att_vals, on=['WS_Node_Name', 'WS_SKU', 'WS_Attribute_Name'])

                if 'List_of_Values' in ws_df.columns:
                    data_out(ws_df, node)
                    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    
                
        else:
            print('\n{} No true multivalues'.format(node))
        
elif data_type == 'sku':
    start_time = time.time()

    if len(search_data)>4000:
        num_lists = round(len(search_data)/4000, 0)
        num_lists = int(num_lists)
    
        if num_lists == 1:
            num_lists = 2
        print('running SKUs in {} batches'.format(num_lists))

        size = round(len(search_data)/num_lists, 0)
        size = int(size)

        div_lists = [search_data[i * size:(i + 1) * size] for i in range((len(search_data) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            print('batch {} of {}'.format(k+1, num_lists))
            sku_str = ", ".join("'" + str(i) + "'" for i in div_lists[k])

            temp_sku_df = gws.gws_q(gws_values_single, 'tprod."gtPartNumber"', sku_str)
            init_ws_df = pd.concat([init_ws_df, temp_sku_df], axis=0, sort=False) 

    else:
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
        init_ws_df = gws.gws_q(gws_values_single, 'tprod."gtPartNumber"', sku_str)

    ws_skus = init_ws_df['WS_SKU'].unique().tolist()            
    print('{} SKUs'.format(len(ws_skus)))

    for sku in ws_skus:
        temp_ws = init_ws_df.loc[init_ws_df['WS_SKU']== sku]
        temp_grainger = allCATS_df.loc[allCATS_df['WS_SKU']== sku]
        print('found {} in Grainger node {}'.format(sku, temp_grainger['STEP_Category_Name'].unique()))
        temp_grainger = temp_grainger[['WS_SKU', 'WS_Attribute_Name', 'Grainger_Attribute_Value']]
        cols = ['WS_SKU', 'WS_Attribute_Name']
        temp_ws = temp_ws.join(temp_grainger.set_index(cols), on=cols)
        
        ws_df = pd.concat([ws_df, temp_ws], axis=0, sort=False)  
                        
    temp_ws = concat_values(temp_ws)                
    att_vals = pd.concat([att_vals, temp_ws], axis=0)

    ws_df = process_vals(ws_df)

    if att_vals.empty==False:            
        ws_df = pd.merge(ws_df, att_vals, on=['WS_Node_Name', 'WS_SKU', 'WS_Attribute_Name'])

    if 'List_of_Values' in ws_df.columns:
        data_out(ws_df, 'sku')
        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))