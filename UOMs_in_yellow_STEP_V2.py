# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 20:09:28 2020

@author: xcxg109
"""
1# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import pandas as pd
from GWS_query import GWSQuery
from grainger_query import GraingerQuery
import file_data_GWS as fd
import time
import math
import settings_NUMERIC as settings
import WS_query_code as q

pd.options.mode.chained_assignment = None

gcom = GraingerQuery()
gws = GWSQuery()

#NOTE: Not used here, but included to paste into PgAdmin
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
          array_to_string(tax.ancestor_names || tax.name,' > ') as "_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , pi_mappings.step_category_ids[1] AS "STEP_Category_ID"
        , tax_att.id as "WS_Attr_ID"
        , pi_mappings.step_attribute_ids[1] as "STEP_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
        
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND (4458 = ANY(tax.ancestors))
        AND tprod.status = 3

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    INNER JOIN pi_mappings
        ON pi_mappings.gws_attribute_ids[1] = tax_att.id
        AND pi_mappings.gws_category_id = tax_att."categoryId"

        """



def gws_data(grainger_df):
    #take SKUs from grainger_df and use them to pull GWS data
    sku_list = grainger_df['Grainger_SKU'].unique().tolist()
    print('SKUs to query = ', len(sku_list))
    
    gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    gws_df = gws.gws_q(gws_attr_values, 'tprod."gtPartNumber"', gws_skus)

    print('cleaning DF')
    gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].str.replace('_ATTR', '')
    gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].str.replace('_GATTR', '')
    gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].str.strip()
    gws_df['STEP_Attr_ID'] = gws_df['STEP_Attr_ID'].astype(int)

    return gws_df


def grainger_data(gws_df):
    #take SKUs from GWS_df and use them to pull Grainger data    
    sku_list = gws_df['WS_SKU'].unique().tolist()    
    print('SKUs to query = ', len(sku_list))

    grainger_skus = ", ".join("'" + str(i) + "'" for i in sku_list)    
    df = gcom.grainger_q(grainger_attr_query, 'item.MATERIAL_NO', grainger_skus )

    return df


def search_type():
    """choose which type of data to import -- impacts which querries will be run"""
    while True:
        try:
            data_type = input("Search by: \n1. Grainger Blue \n2. Grainger Yellow \n3. GWS \n4. SKU ")
            if data_type in ['1']:
                data_type = 'grainger_query'
                break
            if data_type in ['2']:
                data_type = 'yellow'
                break
            elif data_type in ['3']:
                data_type = 'gws_query'
                break
            elif data_type in ['4']:
                data_type = 'sku'
                break
        except ValueError:
            print('Invalid search type')
        
    return data_type


def process_vals(df):
    """ clean up the sample values column """

    df['Potential_Replaced_Values'] = ''
    df['Revised_Value'] = ''

    for row in df.itertuples():        
        search_string = ''

        orig_value = df.at[row.Index,'Grainger_Attribute_Value']
        orig_value = str(orig_value)

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

        if 'HP' in pot_value or 'hp' in [pot_value]:
            search_string = search_string+'; '+'HP'
            pot_value = pot_value.replace('HP', '')

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
            
        if '  ' in pot_value:
            pot_value = pot_value.replace('  ', ' ')
            
        if ' °' in pot_value:
            pot_value = pot_value.replace(' °', '°')

        if '° ' in pot_value:
            pot_value = pot_value.replace('° ', '°')

        search_string = search_string[2:]
        pot_value = pot_value.strip()

        df.at[row.Index,'Potential_Replaced_Values'] = search_string
        df.at[row.Index,'Revised_Value'] = pot_value

    return df    


def yellow_match(grainger_df, yellow_df):
    #when querying WS first, drop rows from STEP that don't return a result
    grainger_df.dropna(subset=['Segment_ID'], inplace=True)

    grainger_df['Category_ID'] = grainger_df['Category_ID'].astype(int)
    grainger_df['Grainger_Attr_ID'] = grainger_df['Grainger_Attr_ID'].astype(int)

    grainger_df['concat'] = grainger_df['Category_ID'].map(str) + '_' + grainger_df['Grainger_Attr_ID'].map(str)

    grainger_df['Filter'] = 'N'
    grainger_df['Table'] = 'N'

    grainger_df = pd.merge(grainger_df, yellow_df, how='left', left_on='concat', right_on='Identifier')
 
    for row in grainger_df.itertuples():
        f = grainger_df.at[row.Index,'Yellow Folder Category Attribute Rank']
        t = grainger_df.at[row.Index,'Web Table Rank']

        if math.isnan(f):
            pass
        else:
            grainger_df.at[row.Index,'Filter'] = 'Y'

        if math.isnan(t):
            pass
        else:
            grainger_df.at[row.Index,'Table'] = 'Y'
            
#    grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/test.csv')
    
    return grainger_df
    

def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def data_out(final_df, node, batch=''):
    final_df = final_df[final_df.Potential_Replaced_Values != '']
    final_df = final_df[final_df.Grainger_Attribute_Name != 'Item']
    
    final_df = final_df.sort_values(['Potential_Replaced_Values'], ascending=[True])
    
    final_df['concat'] = final_df['Grainger_Attribute_Name'].map(str) + final_df['Grainger_Attribute_Value'].map(str)
    final_df['Group_ID'] = final_df.groupby(final_df['concat']).grouper.group_info[0] + 1
    final_df = final_df[['Group_ID', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                'Category_Name', 'WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'PM_Code', \
                'Sales_Status', 'Relationship_MGR_Code', 'Grainger_SKU', 'WS_SKU', 'Filter', 'Table' , 'WS_Attr_ID', \
                'WS_Attr_Value_ID', 'WS_Attribute_Name', 'WS_Original_Value', 'Grainger_Attr_ID', \
                'Grainger_Attribute_Name', 'Grainger_Attribute_Value', 'Potential_Replaced_Values', 'Revised_Value']]

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

    col_widths = get_col_widths(final_no_dupes)
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

    col_widths = get_col_widths(final_df)
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


# read in grainger data
print('Choose Grainger L1 file')
allCATS_df = q.get_att_values()

# read in and clean WS data
print('\nChoose WS file')
WS_allCATS_df = q.get_att_values()

WS_allCATS_df['STEP_Attr_ID'] = WS_allCATS_df['STEP_Attr_ID'].str.replace('_ATTR', '')
WS_allCATS_df['STEP_Attr_ID'] = WS_allCATS_df['STEP_Attr_ID'].str.replace('_GATTR', '')
WS_allCATS_df['STEP_Attr_ID'] = WS_allCATS_df['STEP_Attr_ID'].str.strip()
WS_allCATS_df.dropna(subset=['STEP_Attr_ID'], inplace=True)
WS_allCATS_df['STEP_Attr_ID'] = WS_allCATS_df['STEP_Attr_ID'].astype(int)

WS_allCATS_df['STEP_Category_ID'] = WS_allCATS_df['STEP_Category_ID'].str.replace('_DIV1', '')
WS_allCATS_df['STEP_Category_ID'] = WS_allCATS_df['STEP_Category_ID'].str.strip()
WS_allCATS_df['STEP_Category_ID'] = WS_allCATS_df['STEP_Category_ID'].astype(int)

print('working...')
start_time = time.time()

# read in grainger data
yellow_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/code/yellow.csv')

node_ids = allCATS_df['Category_ID'].unique().tolist()

print('number of nodes = ', len(node_ids))

num_lists = input('Number of files to split into? ')
num_lists = int(num_lists)
            
print('running Nodes in {} batches'.format(num_lists))

size = math.ceil(len(node_ids)/num_lists)
size = int(size)

div_list = [node_ids[i * size:(i + 1) * size] for i in range((len(node_ids) + size - 1) // size)]

node = allCATS_df['Segment_ID'].unique()

for k in range(0, len(div_list)):
    print('\n\nBATCH ', k+1)
    count = 1
                    
    grainger_df = pd.DataFrame()      # reset grainger_df to empty

    gws_df = pd.DataFrame()             # reset gws_df to empty
                
    for j in div_list[k]:
        print('batch {} -- {} : {}'.format(k+1, count, j))
        temp_df = allCATS_df.loc[allCATS_df['Category_ID']== j]

        temp_df['Count'] =1
        temp_df['Potential_Replaced_Values'] = ''
        temp_df['Revised Value'] = ''

        count = count + 1
                    
        if temp_df.empty == False:
            gws_df = WS_allCATS_df.loc[WS_allCATS_df['STEP_Category_ID']== j]
    
            if gws_df.empty == False:
                temp_df = pd.merge(temp_df, gws_df, how="left", left_on=['Grainger_SKU', 'Grainger_Attr_ID'], \
                                                                right_on=['WS_SKU', 'STEP_Attr_ID'])

            else:
                print('No GWS SKUs')
    
            grainger_df = pd.concat([grainger_df, temp_df], axis=0, sort=False)

        else:
            print('{} No attribute data'.format(node))                

    grainger_df = yellow_match(grainger_df, yellow_df)

#    grainger_df.to_csv('C:/Users/xcxg109/NonDriveFiles/test.csv')
    grainger_df = grainger_df[['Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                               'Category_Name', 'WS_Category_ID', 'WS_Category_Name', 'WS_Node_ID', \
                               'WS_Node_Name', 'PM_Code', 'Sales_Status', 'Relationship_MGR_Code', \
                               'Grainger_SKU', 'WS_SKU', 'Filter', 'Table' , 'WS_Attr_ID', 'WS_Attr_Value_ID', \
                               'WS_Attribute_Name', 'WS_Original_Value', 'Grainger_Attr_ID', \
                               'Grainger_Attribute_Name', 'Grainger_Attribute_Value']]

    grainger_df = process_vals(grainger_df)

    data_out(grainger_df, node, k+1)
    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))