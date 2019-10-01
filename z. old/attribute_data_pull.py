# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 16:04:58 2019

@author: xcxg109
"""

import pandas as pd
import numpy as np
import glob
import os
import string
import query_code as q
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
#from nltk.stemp.porter import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split


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


def get_stats(df):
    """return unique values for each attribute with a count of how many times each is used in the node"""
    df['Count'] =1
    stats = pd.DataFrame(df.groupby(['Attribute', 'Attribute_Value'])['Count'].sum())
    return stats


def process_att(attribute):
    """text processing of attributes to facilitate matching"""
    attribute = attribute.str.lower()
    #attribute = attribute.to_string(na_rep='').lower()
    attribute = attribute.str.strip()
    attribute = attribute.str.replace('[^\w\s]','')    
    return attribute

    
def check_element(a, b):
  return not set(a).isdisjoint(b)

def lowercase(col):
#    col = col.astype(str).replace(r'\s*\.\s*', np.nan, regex=True)
  #  col = col.astype(str).replace(r'\s*\.\s*', regex=True)
    col = col.str.lower()    
    return col

        
def remote_punctuation(text):
    no_punct = "".join([c for c in text if c not in string.punctuation])
    return no_punct

def remove_stopwords(text):
    words = [w for w in text if w not in stopwords.words('english')]
    return words


def word_lemmatizer(text):
    lemmatizer = WordNetLemmatizer()
    
    lem_text = [lemmatizer.lemmatize(i) for i in text]
    return lem_text

def word_stemmer(text):
    stemmer = PorterStemmer()
    
    stem_text = " ".join([stemmer.stem(i) for i in text])
    return stem_text


def jaccard_similarity(query, document):
    intersection = set(query).intersection(set(document))
    union = set(query).union(set(document))
    return len(intersection)/len(union)


def match_values(grainger_val, gamut_vals, min_score=0):
    # -1 score incase we don't get any matches
    max_score = -1
    # Returning empty name for no match as well
    max_name = ""
    # Iternating over all names in the other
    for gamut_vals2 in gamut_vals:
        #Finding fuzzy match score
        score = fuzz.ratio(grainger_val, grainger_vals)
        # Checking if we are above our threshold and have a better score
        if (score > min_score) & (score > max_score):
            max_name = grainger_val2
            max_score = score
    return max_name, max_score
  
    
def cat_filter(df, category, cat_filter):
    cat_filter = df.loc[df[category]== cat_filter]
    return cat_filter


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


def determine_match(df):
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