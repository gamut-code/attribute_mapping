# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np

from pathlib import Path
from collections import defaultdict
 from grainger_query import GraingerQuery

""" __________________SWITCH BETWEEN GWS AND TOOLBOX ENVIRONMENT__________________________"""
from GWS_TOOLBOX_query import gws_q
#from GWS_query import GWSQuery
""" ______________________________ """

from queries_NUMERIC import gamut_attr_query, grainger_attr_query, gamut_attr_values, gamut_basic_query, grainger_basic_query
import data_process as process
import file_data_att as fd
from typing import Dict
import settings
import time

pd.options.mode.chained_assignment = None

""" __________________SWITCH BETWEEN GWS AND TOOLBOX ENVIRONMENT__________________________"""
gamut = gws_q()
#gamut = GWSQuery()
""" ______________________________ """

gcom = GraingerQuery()

pd.options.mode.chained_assignment = None


def match_category(df):
    """compare data colected from matching file (match_df) with grainger and gamut data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""
    
    df['Matching'] = 'no'
    
    for row in df.itertuples():
        grainger_string = str(row.Grainger_Attribute_Name)
        gamut_string = str(row.Gamut_Attribute_Name)
        
        if (grainger_string) == (gamut_string):
            df.at[row.Index,'Matching'] = 'Match'
        elif (grainger_string) in (gamut_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif (gamut_string) in (grainger_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif process.isBlank(row.Grainger_Attribute_Name) == False:
            if process.isBlank(row.Gamut_Attribute_Name) == True:
                df.at[row.Index,'Matching'] = 'Grainger only'
        elif process.isBlank(row.Grainger_Attribute_Name) == True:
            if process.isBlank(row.Gamut_Attribute_Name) == False:
                df.at[row.Index,'Matching'] = 'Gamut only'
            
    return df


def grainger_nodes(node, search_level):
    """basic pull of all nodes if L2 or L3 is chosen"""
    df = pd.DataFrame()
    #pull basic details of all SKUs -- used for gathering L3s if user chooses L2 or L1
    df = gcom.grainger_q(grainger_basic_query, search_level, node)
    
    return df


def ga_skus(grainger_skus):
    """get basic list of gamut SKUs to pull the related PIM nodes"""
    gamut_sku_list = pd.DataFrame()
    
    sku_list = grainger_skus['Grainger_SKU'].tolist()
    
    if len(sku_list)>20000:
        num_lists = round(len(sku_list)/20000, 0)
        num_lists = int(num_lists)
    
        if num_lists == 1:
            num_lists = 2
        print('running SKUs in {} batches'.format(num_lists))

        size = round(len(sku_list)/num_lists, 0)
        size = int(size)

        div_lists = [sku_list[i * size:(i + 1) * size] for i in range((len(sku_list) + size - 1) // size)]

        for k  in range(0, len(div_lists)):
            gamut_skus = ", ".join("'" + str(i) + "'" for i in div_lists[k])
            temp_df = gamut.gws_q(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)
            gamut_sku_list = pd.concat([gamut_sku_list, temp_df], axis=0, sort=False) 
    else:
        gamut_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
        gamut_sku_list = gamut.gws_q(gamut_basic_query, 'tprod."supplierSku"', gamut_skus)

    return gamut_sku_list


def gamut_atts(query, node, query_type):
    """pull gamut attributes based on the PIM node list created by gamut_skus"""
    df = pd.DataFrame()
    #pull attributes for the next pim node in the gamut list
    
    df = gamut.gws_q(query, query_type, node)
    
    print('GWS ', node)

    return df


def gamut_process(node, gamut_dict: Dict, k):
    """if gamut node has not been previously processed (in gamut_dict), process and add it to the dictionary"""

    gamut_df = gamut_atts(gamut_attr_query, node, 'tax.id')  #tprod."categoryId"')  #get gamut attribute values for each gamut_l3 node\

    if gamut_df.empty==False:
        gamut_df = gamut_df.drop_duplicates(subset='Gamut_Attr_ID')  #gamut attribute IDs are unique, so no need to group by pim node before getting unique
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge

        gamut_dict[node] = gamut_df #store the processed df in dict for future reference 
    else:
        print('{} EMPTY DATAFRAME'.format(node))    
        
    return gamut_dict, gamut_df


def grainger_assign_nodes (grainger_df, gamut_df, node):
    """assign gamut node data to grainger columns"""
    
    att_list = []
    
    node_ID = gamut_df['Gamut_Node_ID'].unique()
    cat_ID = gamut_df['Gamut_Category_ID'].unique()
    cat_name = gamut_df['Gamut_Category_Name'].unique()
    node_name = gamut_df['Gamut_Node_Name'].unique()
    pim_path = gamut_df['Gamut_PIM_Path'].unique()

    atts = grainger_df['Grainger_Attribute_Name'].unique()
    att_list = [att for att in atts if att]
    att_list = np.char.strip(att_list)

    for att in att_list:
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Node_ID'] = node_ID
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Category_ID'] = cat_ID
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Category_Name'] = cat_name
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_Node_Name'] = node_name
        grainger_df.loc[grainger_df.Grainger_Attribute_Name == att, 'Gamut_PIM_Path'] = pim_path
    
    return grainger_df


def gamut_assign_nodes (grainger_df, gamut_df):
    """assign grainger node data to gamut columns"""
    
    att_list = []
    
    blue = grainger_df['Grainger Blue Path'].unique()
    seg_ID = grainger_df['Segment_ID'].unique()
    seg_name = grainger_df['Segment_Name'].unique()
    fam_ID = grainger_df['Family_ID'].unique()
    fam_name = grainger_df['Family_Name'].unique()
    cat_ID = grainger_df['Category_ID'].unique()
    cat_name = grainger_df['Category_Name'].unique()
    
    atts = gamut_df['Gamut_Attribute_Name'].unique()
    att_list = [att for att in atts if att]
    att_list = np.char.strip(att_list)
    
    for att in att_list:
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_ID'] = cat_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Grainger Blue Path'] = blue
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_ID'] = seg_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Segment_Name'] = seg_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_ID'] = fam_ID
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Family_Name'] = fam_name
        gamut_df.loc[gamut_df['Gamut_Attribute_Name'] == att, 'Category_Name'] = cat_name

    return gamut_df


def grainger_process(grainger_df, gamut_dict: Dict, k):
    """create a list of grainger skus, run through through the gamut_skus query and pull gamut attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""

    df = pd.DataFrame()

    cat_name = grainger_df['Category_Name'].unique()
    cat_name = list(cat_name)
    cat_name = cat_name.pop()
    print('cat name = {} {}'.format(k, cat_name))

    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gamut query
    grainger_sku_count = len(grainger_skus)
    print('grainger sku count = ', grainger_sku_count)
    grainger_df['Grainger Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']

    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns
    grainger_df['alt_grainger_name'] = process.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge

    gamut_skus = ga_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull
    
    if gamut_skus.empty==False:
        #create a dictionary of the unique gamut nodes that corresponde to the grainger node 
        gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()  #create list of pim nodes to pull
        print('GWS L3s ', gamut_l3)

        for node in gamut_l3:
            if node in gamut_dict:
                gamut_df = gamut_dict[node]
                print ('node {} in gamut dict'.format(node))

            else:
                gamut_dict, gamut_df = gamut_process(node, gamut_dict, k)

            if gamut_df.empty==False:
                node_name = gamut_df['Gamut_Node_Name'].unique()
                node_name = list(node_name)
                node_name = node_name.pop()
                print('node name = {} {}'.format(node, node_name))
                #add correlating grainger and gamut data to opposite dataframes
                grainger_df = grainger_assign_nodes(grainger_df, gamut_df, node)
                gamut_df = gamut_assign_nodes(grainger_df, gamut_df)

                skus = gamut_skus[gamut_skus['Gamut_Node_ID'] == node]
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'Grainger Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 
                temp_df['grainger_sku_count'] = grainger_sku_count
                temp_df['gamut_sku_count'] = len(skus)
                temp_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '+node_name
                temp_df['Gamut/Grainger SKU Counts'] = temp_df['gamut_sku_count'].map(str)+' / '+temp_df['grainger_sku_count'].map(str)

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')

            else:
                print('GWS Node {} EMPTY DATAFRAME'.format(node))

    else:
        grainger_df['Gamut/Grainger SKU Counts'] = '0 / '+str(grainger_sku_count)
        grainger_df['Grainger-Gamut Terminal Node Mapping'] = cat_name+' -- '
        df = grainger_df
        print('No Gamut SKUs for Grainger node {}'.format(k))
        
    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes


def attribute_process(data_type, search_data):
    gamut_df = pd.DataFrame()
    grainger_df = pd.DataFrame()
    grainger_skus = pd.DataFrame()
    attribute_df = pd.DataFrame()
    gamut_dict = dict()

    start_time = time.time()
    print('working...')

    if data_type == 'grainger_query':
        if search_level == 'cat.CATEGORY_ID':
            for k in search_data:
                grainger_df = gcom.grainger_q(grainger_attr_query, search_level, k)

                if grainger_df.empty == False:
                    temp_df, gamut_dict = grainger_process(grainger_df, gamut_dict, k)
                    attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                    print ('Grainger node = ', k)
                else:
                    print('No attribute data')

        else:
            for k in search_data:
                print('K = ', k)
                grainger_skus = q.grainger_nodes(k, search_level)
                grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
                print('grainger L3s = ', grainger_l3)

                for j in grainger_l3:
                    grainger_df = q.gcom.grainger_q(grainger_attr_query, 'cat.CATEGORY_ID', j)

                    if grainger_df.empty == False:
                        temp_df, gamut_dict = grainger_process(grainger_df, gamut_dict, j)
                        attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
                        print ('Grainger ', j)
                    else:
                        print('Grainger node {} All SKUs are R4, R9, or discontinued'.format(j)) 

                print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

        print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

    return attribute_df

def modify_name(df, replace_char, replace_with):
    return df.str.replace(replace_char, replace_with)


def outfile_name (directory_name, quer, df, search_level, gamut='no'):
#generate the file name used by the various output functions
    if search_level == 'SKU':
        outfile = Path(directory_name)/"SKU REPORT.xlsx"
    else:
        if search_level == 'cat.SEGMENT_ID':    #set directory path and name output file
            outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,2], df.iloc[0,3], quer)
        elif search_level == 'cat.FAMILY_ID':
            outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,4], df.iloc[0,5], quer)
        else:
            outfile = Path(directory_name)/"{} {} {}.xlsx".format(df.iloc[0,6], df.iloc[0,7], quer)

    return outfile


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def data_out(directory_name, att_df, search_level):
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    att_df['Category_Name'] = modify_name(df['Category_Name'], '/', '_') #clean up node names to include them in file names       

    quer = 'GRAINGER-GAMUT'
    
    #output raw files before organizing/editing columns for human file

    columnsTitles = ['Gamut/Grainger SKU Counts', 'Grainger Blue Path', 'Segment_ID', 'Segment_Name', \
                     'Family_ID', 'Family_Name', 'Category_ID', 'Category_Name', 'Gamut_PIM_Path', \
                     'Gamut_Category_ID', 'Gamut_Category_Name', 'Gamut_Node_ID', 'Gamut_Node_Name', \
                     'Grainger-Gamut Terminal Node Mapping', 'Grainger_Attr_ID', 'Grainger_Attribute_Name',\
                     'Gamut_Attr_ID', 'Gamut_Attribute_Name', 'Matching']
    
    att_df = att_df.reindex(columns=columnsTitles)
    outfile = outfile_name (directory_name, quer, att_df, search_level)

    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    
    pd.io.formats.excel.header_style = None
    
    # Write each dataframe to a different worksheet.
    att_df = att_df.sort_values(['Segment_Name', 'Category_Name', 'Gamut_Node_Name', 'Grainger_Attribute_Name', \
                         'Gamut_Attribute_Name'], ascending=[True, True, True, True, True])
    att_df.to_excel(writer, sheet_name='Attrbute Data', index=False)
    
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    worksheet = writer.sheets['Attribute Data']
    
    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')
    
    header_fmt = workbook.add_format()
    header_fmt.set_text_wrap('text_wrap')
    header_fmt.set_bold()

    num_layout = workbook.add_format()
    num_layout.set_num_format('##0.00')
    
    col_widths = get_col_widths(df)
    col_widths = col_widths[1:]

    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet.set_column(i, i, width) 
    writer.save()

#determine SKU or node search
search_level = 'cat.CATEGORY_ID'
data_type = fd.search_type()

print('working...')

if data_type == 'grainger_query':
    search_level, data_process = fd.blue_search_level()

    search_data = fd.data_in(data_type, settings.directory_name)
    attribute_df = attribute_process(data_type, search_data)

    if attribute_df.empty==False:
        data_out(settings.directory_name, attribute_df, search_level)
    else:
        print('EMPTY DATAFRAME')

elif data_type == 'gamut_query':
    search_level = 'tax_att."categoryId"'
    search_data = fd.data_in(data_type, settings.directory_name)

    attribute_df = attribute_process(data_type, search_data)

    if attribute_df.empty==False:
        data_out(settings.directory_name, attribute_df, search_level)
    else:
        print('EMPTY DATAFRAME')
