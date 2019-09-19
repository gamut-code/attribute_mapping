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


def cat_filter(df, category, cat_filter):
    cat_filter = df.loc[df[category]== cat_filter]    
    return cat_filter


def lowercase(col):
    col = col.astype(str).replace(r'\s*\.\s*', np.nan, regex=True)
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
        #for col in df.select_dtypes([np.object]).columns:
#        print('{} \n\n {}'.format(att, prev_match[att]))
        #prev_match[att] = prev_match[att].astype(str).replace(r'\s*\.\s*', np.nan, regex=True)
        #prev_match[att] = prev_match[att].str.lower()
        prev_match[att] = lowercase(prev_match[att])
  #      prev_match[att] = prev_match[att].replace(np.nan, '', regex=True)
        #grainger_att = process_att(grainger_att[att])
        #gamut_att = process_att(gamut_att[att])

    #read in taxonomist approved column and act on yes entries
    #prev_match['Taxonomist Approved (yes/no)'] = prev_match['Taxonomist Approved (yes/no)'].replace(r'\s*\.\s*', np.nan, regex=True)
#    prev_match['Taxonomist Approved (yes/no)'] = prev_match['Taxonomist Approved (yes/no)'].fillna('')
    #prev_match.loc[prev_match['Taxonomist Approved (yes/no)']].str.lower()
    #prev_match['Taxonomist Approved (yes/no)'] = prev_match['Taxonomist Approved (yes/no)'].apply(lambda x: x.str.lower())
    prev_match['Taxonomist Approved (yes/no)'] = lowercase(prev_match['Taxonomist Approved (yes/no)'])
    #prev_match['Taxonomist Approved (yes/no)'] = prev_match['Taxonomist Approved (yes/no)'].replace(r'\s*\.\s*', np.nan, regex=True)
    #prev_match['Taxonomist Approved (yes/no)'] = prev_match['Taxonomist Approved (yes/no)'].str.lower()

    att_split = []
    
    for idx, value in prev_match.iterrows():
        if prev_match['Taxonomist Approved (yes/no)'][idx] == 'yes':
            if prev_match['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'][idx] != '':
                att_split = prev_match['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'][idx].split(';')
             #   att_split = [x.lower for x in att_split]
               # print('att_split ', att_split)
                count = len(att_split)
               # print('count ', count)
                node = prev_match['Gamut_Node_ID'][idx]
               # print('node ', node)
                temp_att_df = q.gamut_atts(node)
                temp_att_df['Gamut_Attribute_Name'] = lowercase(temp_att_df['Gamut_Attribute_Name'])
                node_atts = temp_att_df['Gamut_Attribute_Name'].str.lower().unique()
               # print('node_atts ', node_atts)
                node_atts = node_atts.tolist()
                if count == 1:
               #     if att_split in node_atts:
                    if check_element(att_split, node_atts) == True:
                        print('att == att_split')
                       # prev_match['Gamut_Attribute_Name'][idx] = att_split
                        att_split = att_split.pop()
                        prev_match.loc[idx, 'Gamut_Attribute_Name'] = att_split
                        print('Gamut_Attribute_Name new ', prev_match['Gamut_Attribute_Name'][idx])
                        attribute_ID = cat_filter(temp_att_df, 'Gamut_Attribute_Name', att_split)
                        attribute_ID = attribute_ID['Gamut_Attr_ID'].unique()
                        print('attribute ID ,', attribute_ID)
                        prev_match.loc[idx, 'Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
                        prev_match.loc[idx, 'Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
                        prev_match.loc[idx, 'Gamut_Attr_ID'] = attribute_ID
                        prev_match.loc[idx, 'Status'] = "Match"
                    else:
                        print('Node: {}    Attribute Name = {}    problem name'.format(node, att_split))
            elif count > 1:
                for attribute in att_split:
                    for att in node_atts:
                        if att == attribute:
                            temp_df.loc[prev_match.index[idx]] = prev_match.iloc[idx]
                               # temp_df = prev_match.loc[prev_match['Gamut_Attribute_Name'] == att]
                            tenp_df['Gamut_Attribute_Name'] = att
                            temp_df['Gamut_Node_ID'] = temp_att_df['Gamut_Node_ID']
                            temp['Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
                            prev_match['Status'] = "Match"
                            prev_match = pd.concat([prev_match, temp_df], axis=0, sort=False)
                            prev_match = prev_match.drop(prev_match.index[idx])
            elif prev_match['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'][idx] != '':
                att_split = prev_match['Identified Matching Grainger Attribute Name (use semi-colon to separate names)'][idx].split(';')
             #   att_split = [x.lower for x in att_split]
               # print('att_split ', att_split)
                count = len(att_split)
               # print('count ', count)
                node = prev_match['Grainger_Node_ID'][idx]
               # print('node ', node)
                temp_att_df = q.gamut_atts(node)
                node_atts = temp_att_df['Gamut_Attribute_Name'].str.lower().unique()
               # print('node_atts ', node_atts)
                node_atts = node_atts.tolist()
                if count == 1:
               #     if att_split in node_atts:
                    if check_element(att_split, node_atts) == True:
                       print('att == att_split')
                       # prev_match['Gamut_Attribute_Name'][idx] = att_split
                       print(idx)
                       prev_match.loc[idx, 'Gamut_Attribute_Name'] = att_split
                       print('Gamut_Attribute_Name new ', prev_match['Gamut_Attribute_Name'][idx])
                       prev_match.loc[idx, 'Gamut_Node_ID'] = node
                       prev_match.loc[idx, 'Identified Matching Gamut Attribute Name (use semi-colon to separate names)'] = ""
                       prev_match.loc[idx, 'Identified Matching Grainger Attribute Name (use semi-colon to separate names)'] = ""
                       prev_match.loc[idx, 'Status'] = "Match"
                    else:
                        print('Node: {}    Attribute Name = {}    problem name'.format(node, att_split))


    path = 'F:\CGabriel\Grainger_Shorties\OUTPUT\PREV_MATCH.csv'
    prev_match.to_csv(path)