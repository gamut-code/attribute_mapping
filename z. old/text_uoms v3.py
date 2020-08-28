# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 13:32:56 2020

@author: xcxg109
"""

import pandas as pd
from GWS_query import GWSQuery
import file_data_GWS as fd
import time
import math
import settings_NUMERIC as settings


pd.options.mode.chained_assignment = None

gws = GWSQuery()


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
        , tprodvalue.id
        , tprodvalue."id_migration"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"

    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."dataType" = 'text'
        AND {} IN ({})
        """

gws_basic_query="""
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
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY

    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***
"""




def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def process_vals(df):
    """ clean up the sample values column """
        
    for row in df.itertuples():
        search_string = ''

        orig_value = df.at[row.Index,'Original_Value']
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


def data_out(ws_df, node, batch=''):
    ws_df = ws_df[ws_df.Potential_Replaced_Values != '']
    ws_df = ws_df.sort_values(['Potential_Replaced_Values'], ascending=[True])
                
    ws_df['concat'] = ws_df['WS_Attribute_Name'].map(str) + ws_df['Original_Value'].map(str)
    ws_df['Group_ID'] = ws_df.groupby(ws_df['concat']).grouper.group_info[0] + 1
    ws_df = ws_df[['Group_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'WS_SKU', 'id', 'id_migration', \
                   'WS_Attr_ID', 'WS_Attribute_Name', 'Original_Value', 'Normalized_Value', 'Potential_Replaced_Values',\
                   'Revised Value']]

    ws_no_dupes = ws_df.drop_duplicates(subset=['WS_Attribute_Name', 'Original_Value'])
    ws_no_dupes = ws_no_dupes[['Group_ID', 'WS_Node_ID', 'WS_Node_Name', 'WS_SKU', 'WS_Attr_ID', 'WS_Attribute_Name',\
                               'Original_Value', 'Normalized_Value', 'Potential_Replaced_Values', 'Revised Value']]
    ws_no_dupes = ws_no_dupes.rename(columns={'WS_SKU':'Example SKU'})

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_'+str(batch)+'_text_UOMs.xlsx'  
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook  = writer.book

    ws_no_dupes.to_excel (writer, sheet_name="Uniques", startrow=0, startcol=0, index=False)
    ws_df.to_excel (writer, sheet_name="All Text UOMs", startrow=0, startcol=0, index=False)

    worksheet1 = writer.sheets['Uniques']
    worksheet2 = writer.sheets['All Text UOMs']

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    col_widths = get_col_widths(ws_no_dupes)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    worksheet1.set_column('G:G', 50, layout)
    worksheet1.set_column('H:H', 50, layout)
    worksheet1.set_column('J:J', 50, layout)

    col_widths = get_col_widths(ws_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet2.set_column(i, i, width)

    worksheet2.set_column('J:J', 50, layout)
    worksheet2.set_column('K:K', 50, layout)
    worksheet2.set_column('M:M', 50, layout)

    writer.save()



df = pd.DataFrame()
ws_df = pd.DataFrame()
ws_no_dupes = pd.DataFrame()
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

if data_type == 'gws_query':
    start_time = time.time()

    if search_level == 'single':
        for node in search_data:
            df = gws.gws_q(gws_values_single, 'tprod."categoryId"', node)

            if df.empty == False:
                node_ids = df['WS_Node_ID'].unique().tolist()

                df['Potential_Replaced_Values'] = ''
                df['Revised Value'] = ''
            
                for n in node_ids:
                    print(n)
                    temp_df = df.loc[df['WS_Node_ID']== n]
                    temp_df['Count'] =1
                    temp_df = process_vals(temp_df)

                    ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df

                data_out(ws_df, node)
                print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    

            else:
                print('{} No attribute data'.format(node))

    elif search_level == 'group':
        for node in search_data:
            df = gws.gws_q(gws_basic_query, 'tprod."categoryId"', node)           

            print('k = ', node)
    
            if df.empty == False:
                node_ids = df['WS_Node_ID'].unique().tolist()
                print('number of nodes = ', len(node_ids))
            
                if len(node_ids)>80:
                    num_lists = round(len(node_ids)/80, 0)
                    num_lists = int(num_lists)
    
                    if num_lists == 1:
                        num_lists = 2
                    print('running WS Nodes in {} batches'.format(num_lists))

                    size = math.ceil(len(node_ids)/num_lists)
                    size = int(size)

                    div_list = [node_ids[i * size:(i + 1) * size] for i in range((len(node_ids) + size - 1) // size)]

                    for k in range(0, len(div_list)):
                        print('\n\nBATCH ', k+1)
                        count = 1
                        
                        ws_df = pd.DataFrame()      # reset ws_df to empty
                    
                        for j in div_list[k]:
                            print('batch {} -- {} : {}'.format(k+1, count, j))
                            temp_df = gws.gws_q(gws_values_single, 'tprod."categoryId"', j)

                            temp_df['Count'] =1
                            temp_df['Potential_Replaced_Values'] = ''
                            temp_df['Revised Value'] = ''

                            temp_df = process_vals(temp_df)
                            count = count + 1
                            
                            ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df

                        data_out(ws_df, node, k+1)
                        print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

                else:
                    count = 1

                    for n in node_ids:
                        print('{} -- {}'.format(count, n))

                        temp_df = gws.gws_q(gws_values_single, 'tprod."categoryId"', n)

                        temp_df['Count'] =1
                        temp_df['Potential_Replaced_Values'] = ''
                        temp_df['Revised Value'] = ''

                        temp_df = process_vals(temp_df)
                        count = count + 1

                        ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df

                    data_out(ws_df, node)
                    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    

            else:
                print('{} No attribute data'.format(node))

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
            df = pd.concat([df, temp_sku_df], axis=0, sort=False) 

    else:
        sku_str = ", ".join("'" + str(i) + "'" for i in search_data)
        df = gws.gws_q(gws_values_single, 'tprod."gtPartNumber"', sku_str)
        

    if df.empty == False:
        atts = df['WS_Attr_ID'].unique()

        df['Potential_Replaced_Values'] = ''
        df['Revised Value'] = ''

        for attribute in atts:
            temp_df = df.loc[df['WS_Attr_ID']== attribute]
            temp_df = process_vals(temp_df)
                
            ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df
                
        data_out(ws_df, ws_no_dupes, 'sku')

    else:
        print('{} No attribute data'.format(node))

    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))    

