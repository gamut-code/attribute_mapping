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
from collections import defaultdict

from nltk.tokenize import TreebankWordTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
#from nltk.stemp.porter import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split

import csv



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


def isBlank (myString):
    return (myString and pd.isnull(myString))

    
def check_element(a, b):
  return not set(a).isdisjoint(b)


def lowercase(col):
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


def tokenize(text):
    tokenizer = TreebankWordTokenizer()
    
    tokenizer.tokenize(text)

def jaccard_similarity(query, document):
    intersection = set(query).intersection(set(document))
    union = set(query).union(set(document))
    return len(intersection)/len(union)


def get_score(grainger_val, gamut_vals, min_score=0):
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


def attribute_name_match(df):

#    grainger_atts = {}
    grainger_att = defaultdict(lambda: defaultdict())
#    gamut_atts = {}
    vals = pd.DataFrame()
  
    grainger_nodes = df['Category_ID'].unique()    
 #   for node in nodes:
  #      atts = df['Grainger_Attribute_Name'].unique()
   #     print('atts = ', atts)
    #    for attribute in atts:
 #           vals = cat_filter(df, 'Grainger_Attribute_Name', attribute)
  #          vals = vals['Grainger ALL Values'].unique()
            
   #         grainger_vals[attribute] = vals
    #    grainger_atts[node] = grainger_vals
        
        #grainger_atts[node] = [{str(df['Grainger_Attribute_Name'][i]): df['Grainger ALL Values'][i]} for i in df[df['Category_ID']==node].index]

#    for node in df['Gamut_Node_ID'].unique():
 #       gamut_atts[node] = [{str(df['Gamut_Attribute_Name'][i]): df['Gamut ALL Values'][i]} for i in df[df['Gamut_Node_ID']==node].index]
    
    
  #  grainger_atts = {key: value for key, value in )}
#    w = csv.writer(open('F:\CGabriel\Grainger_Shorties\OUTPUT\test', 'w'))
 #   for key, val in dict.items():
  #      w.writerow([key, val])

    attributes = df.drop_duplicates(subset=['Category_ID', 'Gamut_Node_ID', 'Grainger_Attr_ID', 'Gamut_Attr_ID'])
    attributes = attributes[['Category_ID', 'Gamut_Node_ID', 'Grainger_Attr_ID', 'Grainger_Attribute_Name', 'Gamut_Attr_ID', 'Gamut_Attribute_Name',
                             'Matching', 'Grainger ALL Values', 'Gamut ALL Values']]
    attributes = attributes[attributes['Matching'] != 'Match']
    
 #   with open('F:/CGabriel/Grainger_Shorties/OUTPUT/test.csv', 'w') as f:
  #      for key in grainger_atts.keys():
   #         f.write("%s,%s\n"%(key,grainger_atts[key]))
    attributes.to_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test.csv')
   # return grainger_atts
#    for idx, value in df.iterrows():
        