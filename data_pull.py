# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 17:04:18 2019

@author: xcxg109
"""

import pandas as pd
import glob
import os
import query_code as q



def get_files(path):
    """read in the previous matching files"""
    all_files = glob.glob(path + "\*.csv")

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, encoding='utf-8')
        df['filename'] = os.path.basename(filename)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True, sort=False)

    return df




def check_for_match(prev_match, temp_att_df, idx, count, node, att_split, node_atts, name, id_type):
    """compare the attribute name given in the match column with the list of attributes in the specific node to determine a match"""
    if count == 1:
        if check_element(att_split, node_atts) == True:
            att_split = att_split.pop()
            prev_match.loc[idx, name] = att_split
            print('Matched attribute name: ', prev_match[name][idx])
            attribute_ID = cat_filter(temp_att_df, name, att_split)
        
            if attribute_ID.empty:
                print('attribute ID empty, trying alternate approach')
                if id_type == 'Gamut_Attr_ID':
                    print('WRITE THIS CODE!')
                elif id_type == 'Grainger_Attr_ID':
                    attribute_ID = q.grainger_by_name(att_split)
                
            attribute_ID = attribute_ID[id_type].unique()
            print('attribute ID ', attribute_ID)
            prev_match.loc[idx, 'Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
            prev_match.loc[idx, 'Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
            prev_match.loc[idx, id_type] = attribute_ID
            prev_match.loc[idx, 'Status'] = "Match"
        else:
            print('Node: {}    Attribute Name = {}    problem name'.format(node, att_split))
    if count > 1:
        for attribute in att_split:
            print('LOOP attribute = ', attribute)
            temp_df = pd.DataFrame()
            if check_element(attribute, node_atts) == True:
                #create a tempoerary row for the attribute that is a copy of the prev_match
                temp_df.loc[prev_match.index[idx]] = prev_match.iloc[idx]
                print ('temp_df = ', temp_df)
                # temp_df = prev_match.loc[prev_match['Gamut_Attribute_Name'] == att]
                temp_df[name] = attribute
                print('Matched attribute name: ', temp_df[name])
                attribute_ID = cat_filter(temp_att_df, name, attribute)

                if attribute_ID.empty:
                    print('attribute ID empty, trying alternate approach')
                    if id_type == 'Gamut_Attr_ID':
                        print('WRITE THIS CODE!')
                    elif id_type == 'Grainger_Attr_ID':
                        attribute_ID = q.grainger_by_name(attribute)

                attribute_ID = attribute_ID[id_type].unique()
                print('attribute ID ', attribute_ID)
                temp_df.loc[idx, 'Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
                temp_df.loc[idx, 'Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
                temp_df.loc[id_type] = attribute_ID
                temp_df.loc['Status'] = "Match"
                prev_match = pd.concat([prev_match, temp_df], axis=0, sort=False)
            else:
                print('Node: {}    Attribute Name = {}    problem name'.format(node, attribute))
        prev_match = prev_match.drop(prev_match.index[idx])
       
    return None


def previous_match(df):
    path = r'C:\Users\xcxg109\Documents\GitHub\attribute_mapping\Matching Attribute files'

    prev_match = get_files(path)

#    temp_df = pd.DataFrame()
    sugg_list = pd.read_excel(path + "\suggested_match.xlsx")
    
    att_list = 'Grainger_Attribute_Name', 'Gamut_Attribute_Name', 'Identified Matching Gamut Attribute Name (use semi-colon to separate names)', 'Identified Matching Grainger Attribute Name (use semi-colon to separate names)'
  #  for att in att_list:
  #      prev_match[att] = prev_match[att].fillna("")
  #      print('{} \n\n {}'.format(att, prev_match[att]))
    for att in att_list:
        prev_match[att] = prev_match[att].fillna("")
        prev_match[att] = lowercase(prev_match[att])

    #read in taxonomist approved column and act on yes entries
    prev_match['Taxonomist Approved (yes/no)'] = lowercase(prev_match['Taxonomist Approved (yes/no)'])

    for idx, value in prev_match.iterrows():
        if prev_match['Taxonomist Approved (yes/no)'][idx] == 'yes':
            print('{} Gamut value : {} '.format(idx, prev_match['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'][idx]))
            print('{} Grainger value : {} '.format(idx, prev_match['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'][idx]))
            
            if prev_match['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'][idx] != "":
                att_split = prev_match['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'][idx].split(';')
                print('Gamut att_split ', att_split)
                count = len(att_split)
                print('Gamut count ', count)
                node = prev_match['Gamut_Node_ID'][idx]
                print('Gamut node ', node)
                temp_att_df = q.gamut_atts(node)
#                temp_att_df['Gamut_Attribute_Name'] = lowercase(temp_att_df['Gamut_Attribute_Name'])
                node_atts = temp_att_df['Gamut_Attribute_Name'].str.lower().unique()
                node_atts = node_atts.tolist()
                check_for_match(prev_match, temp_att_df, idx, count, node, att_split, node_atts, 'Gamut_Attribute_Name', 'Gamut_Attr_ID')
            elif prev_match['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'][idx] != "":
                att_split = prev_match['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'][idx].split(';')
                print('Grainger att_split ', att_split)
                count = len(att_split)
                print('Grainger count ', count)
                node = prev_match['Category_ID'][idx]
                print('Grainger node ', node)
                temp_att_df = q.grainger_atts(node)
                node_atts = temp_att_df['Grainger_Attribute_Name'].str.lower().unique()
                node_atts = node_atts.tolist()
                check_for_match(prev_match, temp_att_df, idx, count, node, att_split, node_atts, 'Grainger_Attribute_Name', 'Grainger_Attr_ID')
               #     if att_split in node_atts:
               
    path = 'F:\CGabriel\Grainger_Shorties\OUTPUT\PREV_MATCH.csv'
    prev_match.to_csv(path)