# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 16:04:58 2019

@author: xcxg109
"""

import pandas as pd
import numpy as np
import re
import glob
import os
import math
import string
import query_code as q
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import FeatureUnion, Pipeline
#from sparse_dot_topn import awesome_cossim_topn

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


def lowercase(text):
    text = text.str.lower()    
    return text


def remove_stopwords(text):
    stop_words_list = ['<null>', 'not applicable', 'not available']
#    words = [w for w in text if w not in stopwords.words('english')]
    for w in stop_words_list:
        pattern = r'\b'+w+r'\b'
        print('STOPWORDS TEXT = ', text)
        words = re.sub(pattern, '', text)
    return words


def remove_punc(text):
    remove = string.punctuation
    remove = remove.replace("-", "") # don't remove hyphens
    pattern = r"[{}]".format(remove) # create the pattern
    text = re.sub(pattern, "", text)    
#    text = s.translate(str.maketrans('', '', string.punctuation))
    return text


def word_lemmatizer(text):
    lemmatizer = WordNetLemmatizer()
    
    lem_text = [lemmatizer.lemmatize(i) for i in text]
    return lem_text


def process_values(col):
    col = lowercase(col)
    col = remove_stopwords(col)
    col = remove_punc(col)    
    col = word_lemmatizer(col)
    return col
    
def isBlank (myString):
    return (myString and pd.isnull(myString))

    
def check_element(a, b):
  return not set(a).isdisjoint(b)

        
#def remote_punctuation(text):
 #   no_punct = "".join([c for c in text if c not in string.punctuation])
  #  return no_punct

#def word_stemmer(text):
 #   stemmer = PorterStemmer()
    
 #   stem_text = " ".join([stemmer.stem(i) for i in text])
  #  return stem_text

#def tokenize(text):
 #   tokenizer = TreebankWordTokenizer()    
  #  tokenizer.tokenize(text)

#def jaccard_similarity(query, document):
 #   intersection = set(query).intersection(set(document))
  #  union = set(query).union(set(document))
   # return len(intersection)/len(union)


#def ngrams_analyzer(string):
 #   string = re.sub(r'[,-./]', r'', string)
  #  string = string.lower()
   # string = remove_stopwords(string)
    #ngrams = zip(*[string[i:] for i in range(3)])  # N-Gram length is 5
  #  return [''.join(ngram) for ngram in ngrams]


#def get_score(grainger_val, gamut_vals, min_score=0):
    # -1 score incase we don't get any matches
 #   max_score = -1
    # Returning empty name for no match as well
  #  max_name = ""
    # Iternating over all names in the other
   # for gamut_vals2 in gamut_vals:
        #Finding fuzzy match score
    #    score = fuzz.ratio(grainger_val, grainger_vals)
        # Checking if we are above our threshold and have a better score
     #   if (score > min_score) & (score > max_score):
      #      max_name = grainger_val2
       #     max_score = score
  #  return max_name, max_score
  

#def round_half_up(n, decimals=0):
 #   multiplier = 10 ** decimals
  #  return math.floor(n*multiplier + 0.5) / multiplier

    
def cat_filter(df, category, cat_filter):
    cat_filter = df.loc[df[category]== cat_filter]
    return cat_filter


def attribute_name_match(df):
    for i,j in iterrows():
        df['Grainger ALL Values'] = process_values(df['Grainger ALL Values'])
        df['Gamut ALL Values'] = process_values(df['Gamut ALL Values'])
    
    vect = TfidfVectorizer(sublinear_tf=True, max_df=0.5, analyzer='word', stop_words='english')

    X = vect.fit_transform(df.pop('Grainger ALL Values')).toarray()

    for i, col in enumerate(vect.get_feature_names()):
        df[col] = X[:, i]
        
    df.to_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test2.csv')