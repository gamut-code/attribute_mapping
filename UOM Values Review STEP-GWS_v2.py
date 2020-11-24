# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import re
import time
import math
import file_data_GWS as fd
import WS_query_code as q

pd.options.mode.chained_assignment = None

def gws_values(df):
    df['WS_Value'] = ''
    
    for row in df.itertuples():
        dt = df.at[row.Index, 'Data_Type']
        val = df.at[row.Index, 'Normalized_Value']
        
        if dt == 'number':
            unit = df.at[row.Index, 'Normalized_Unit']
            unit = str(unit)
            
            display = df.at[row.Index, 'Numeric_Display_Type']

            if display == 'fraction':
                numer = df.at[row.Index, 'Numerator']

                try:
                    numer = int(numer)
                except:
                    numer = float(numer)
                    
                denom = df.at[row.Index, 'Denominator']
                
                try:
                    denom = int(denom)
                except:
                    denom= float(denom)
                    
                whole_num = numer//denom

                if whole_num != 0:
                    if denom != 1:
                        remain = numer % denom

                        if unit == '' or unit == 'nan':
                            ws_val = str(whole_num) + ' ' + str(remain) + '/' + str(denom)

                        else:
                            ws_val = str(whole_num) + ' ' + str(remain) + '/' + str(denom) + ' ' + str(unit)  

                    else:
                        if unit == '' or unit == 'nan':
                            ws_val = str(whole_num)
                        else:
                            ws_val = str(whole_num) + ' ' + str(unit)  
                                
                elif denom != 1:
                    if unit == '' or unit == 'nan':
                        ws_val = str(numer) + '/' + str(denom)
                    else:
                        ws_val = str(numer) + '/' + str(denom) + ' ' + str(unit)                    
                        
                else:
                    ws_val = "ERROR: CHECK THIS"
                    
                df.at[row.Index, 'WS_Value'] = ws_val

            elif display == 'decimal':
                # add thousands comma separator to value                
                try:
                    val = int(val)
                except:
                    try:
                        val = float(val)
                    except:
                        val = str(val)
                    
                if type(val) == int or type(val) == float:
                    val = '{:,}'.format(val)

                if unit == '' or unit == 'nan':    
                    ws_val = str(val)
                else:
                    ws_val = str(val) + ' ' + str(unit)
                    
                df.at[row.Index, 'WS_Value'] = ws_val
            
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
        
    if 'degrees' in pot_value or 'Degrees' in pot_value:
        search_string = search_string+'; '+'degrees'
        pot_value = pot_value.replace('degrees', '°')
        pot_value = pot_value.replace('Degrees', '°')
            
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
        pot_value = pot_value.replace('HP', ' HP')

    search_string = search_string[2:]
    pot_value = pot_value.strip()

    return df, search_string, pot_value

            
def compare_values(df):
    df['STEP-WS_Match?'] = ''
    df['Potential_Replaced_Values'] = ''
    df['Revised_Value'] = ''
        
    for row in df.itertuples():
        multi = df.at[row.Index, 'Multivalue?']
        multi = str(multi)
        
        gr_val = df.at[row.Index, 'Grainger_Attribute_Value']
        gr_val = str(gr_val)

        ws_val = df.at[row.Index, 'WS_Value']
        ws_val = str(ws_val)

        orig_value = gr_val

        df, search_string, pot_value = process_vals(df, orig_value)

        if search_string == '' or search_string == 'nan':
            pass   # do nothing -- don't populate 'Revised Values' if no changes needed

        else:
            df.at[row.Index,'Potential_Replaced_Values'] = search_string
            df.at[row.Index,'Revised_Value'] = pot_value

        if ws_val == '' or ws_val == 'nan':
            pass # no comparison if there is no WS value
        
        else:
            if multi.lower() == 'false' and 'degrees' not in search_string:    # do not process comparisons on multivalued attributes, results won't match
                if gr_val == ws_val:
                    df.at[row.Index, 'STEP-WS_Match?'] = 'Y'
                else:
                    df.at[row.Index, 'STEP-WS_Match?'] = 'N'
        
    return df


def data_out(final_df, node, node_name, batch=''):    
    final_df['Potential_Replaced_Values'] = final_df['Potential_Replaced_Values'].str.replace('degrees', '')
    
    # drop rows where STEP and WS data match and we are not recommending a change
    final_df = final_df[(final_df['STEP-WS_Match?'] == 'N') | (final_df['Potential_Replaced_Values'] != '')]
    
    final_df = final_df[final_df.Grainger_Attribute_Name != 'Item']

    final_df = final_df.sort_values(['Potential_Replaced_Values'], ascending=[True])
    
    final_df['concat'] = final_df['Grainger_Attribute_Name'].map(str) + final_df['Grainger_Attribute_Value'].map(str)
    final_df['Group_ID'] = final_df.groupby(final_df['concat']).grouper.group_info[0] + 1

    final_df = final_df[['Group_ID', 'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_ID', \
                'Category_Name', 'PM_Code', 'Sales_Status', 'Relationship_MGR_Code', 'WS_Category_ID', \
                'WS_Category_Name', 'WS_Node_ID', 'WS_Node_Name', 'Grainger_SKU', 'WS_SKU', 'Grainger_Attr_ID', \
                'WS_Attr_ID', 'WS_Attr_Value_ID', 'Multivalue?', 'Data_Type', 'Numeric_Display_Type', \
                'WS_Attribute_Name', 'WS_Attribute_Definition', 'Normalized_Value', 'Normalized_Unit', \
                'Numerator', 'Denominator',  'Grainger_Attribute_Name', 'Grainger_Attribute_Definition', \
                'Grainger_Category_Specific_Definition', 'Grainger_Attribute_Value', 'WS_Value',\
                'STEP-WS_Match?', 'Potential_Replaced_Values', 'Revised_Value']]

    final_no_dupes = final_df.drop_duplicates(subset=['Grainger_Attribute_Name', 'Grainger_Attribute_Value', 'Data_Type'])
    
    final_no_dupes = final_no_dupes [['Group_ID', 'Category_ID', 'Category_Name', 'Grainger_SKU', 'Data_Type', \
               'Numeric_Display_Type', 'WS_Attribute_Name', 'WS_Attribute_Definition', 'Grainger_Attribute_Name', \
               'Grainger_Attribute_Definition', 'Grainger_Category_Specific_Definition', 'Grainger_Attribute_Value', \
               'WS_Value', 'STEP-WS_Match?', 'Potential_Replaced_Values', 'Revised_Value']]
    final_no_dupes = final_no_dupes.rename(columns={'Grainger_SKU':'Example SKU'})

    outfile = 'C:/Users/xcxg109/NonDriveFiles/'+str(node)+'_'+str(node_name)+'_'+str(batch)+'_STEP-WS_Analysis.xlsx'  
    
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


# read in grainger data
print('Choose Grainger L1 file')
allCATS_df = q.get_att_values()

# read in and clean WS data
print('\nChoose WS file')
WS_allCATS_df = q.get_att_values()
#WS_allCATS_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/WS MACHINING.csv')

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

node_ids = allCATS_df['Category_ID'].unique().tolist()

print('number of nodes = ', len(node_ids))

num_lists = input('Number of files to split into? ')
num_lists = int(num_lists)

print('running Nodes in {} batches'.format(num_lists))

size = math.ceil(len(node_ids)/num_lists)
size = int(size)

div_list = [node_ids[i * size:(i + 1) * size] for i in range((len(node_ids) + size - 1) // size)]

node = allCATS_df['Segment_ID'].unique()
node = node[0]

node_name = allCATS_df['Segment_Name'].unique()
node_name = node_name[0]

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
                gws_df = gws_values(gws_df)

                temp_df = pd.merge(temp_df, gws_df, how="left", left_on=['Grainger_SKU', 'Grainger_Attr_ID'], \
                                                                right_on=['WS_SKU', 'STEP_Attr_ID'])

            else:
                temp_df['Multivalue?'] = ''
                temp_df['WS_Value'] = ''

                print('No GWS SKUs')

            print('Node {} rows = {}'.format(j, len(temp_df)))
            temp_df = compare_values(temp_df)   # process values before adding to the final           
            grainger_df = pd.concat([grainger_df, temp_df], axis=0, sort=False)

        else:
            print('{} No attribute data'.format(node))                
                            
    data_out(grainger_df, node, node_name, k+1)
            
    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))