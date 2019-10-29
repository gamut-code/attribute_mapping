# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 23:11:41 2019

@author: xcxg109
"""

import string
from collections import Counter
from pprint import pprint
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from spacy.lang.en import English
from spacy.lang.en.stop_words import STOP_WORDS


def cat_filter(df, category, cat_filter):
    cat_filter = df.loc[df[category]== cat_filter]
    return cat_filter


def get_words(text):    
#    doc = remove_punctuation(text)
    token_list = []
    words = []

    nlp = English()  #load Spacy English tokenizer
    
    doc = nlp(text)
    
    # Create list of word tokens and use Spacy for lemmatization
    for token in doc:
        token.lemma_
        token_list.append(token.text)
    
    for wd in token_list:
        txt = nlp.vocab[wd]
        if txt.is_punct == False:       #remove punctuation
            if txt.is_stop == False:    #remove stopwords from Spacy list
                words.append(wd.lower()) 

    return words


def create_term_dict(attribute):
    """ Returns a tf dictionary for each review whose keys are all 
    the unique words in the review and whose values are their 
    corresponding tf.
    """
    #Counts the number of times the word appears in review
    term_dict = dict()
    for word in attribute:
        if word in term_dict:
            term_dict[word] += 1
        else:
            term_dict[word] = 1
    #Computes tf for each word           
    for word in term_dict:
        term_dict[word] = term_dict[word] / len(attribute)
        
    return term_dict


def word_counts(term_dict):
    #dictionary of unique words in with count of docs where word appears
    word_dict = dict()
    
    # Run through each review's tf dictionary and increment countDict's (word, doc) pair
    for attribute in term_dict:
        print('attribute = ', attribute)
        for word in attribute:
            if word in word_dict:
                word_dict[word] += 1
            else:
                word_dict[word] = 1
                
    return word_dict


def grainger_corp(df, node):
    """build a unique document corpus for each grainger node, whith each attribute considered a document"""
    corpus = []
    corp_dict = dict()
    words = []
    clean_words = []
    grainger_term_dict = dict()
    grainger_term_dict[node] = dict()

    #create temp dataframe filters for each node
    temp_df = cat_filter(df, 'Category_ID', node)  
    temp_df = temp_df.dropna(axis=0, subset=['Grainger ALL Values'])
    attributes = temp_df['Grainger_Attribute_Name'].unique()
    #create a "combined" column that includes all values, attribute names, and attribute definitions to define the corpus
    cols = ['Grainger ALL Values', 'Grainger_Attribute_Name', 'Grainger_Attribute_Definition', 'Grainger_Category_Specific_Definition']
    temp_df['combined'] = temp_df[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    #treat each attribute as a separate document and build a corpus
        
    for att in attributes:
        #create a second temp dataframe filtered at the attribute level
        temp2_df = cat_filter(temp_df, 'Grainger_Attribute_Name', att)
        temp2_df = temp2_df.drop_duplicates(subset=['Grainger_Attribute_Name'])
        #create a document out of each attribute (combo of name, definitions, and values)
        doc = temp2_df['combined'].str.cat(sep=' ')
        #process doc into tokens, remove punctuation and stopwords, lemmatize, and remove 'nans'
        words = get_words(doc)
        clean_words = [x for x in words if str(x) != 'nan']
        if len(clean_words) > 0:
            corpus.append(clean_words)
        corp_dict[att] = clean_words
        grainger_term_dict[att] = create_term_dict(clean_words)
        
    return corpus, grainger_term_dict


def gamut_corp(df, node):
    """build a unique document corpus for each grainger node, whith each attribute considered a document"""
    corpus = []
    corp_dict = dict()
    words = []
    clean_words = []
    gamut_term_dict = dict()
    gamut_term_dict[node] = dict()

    #create temp dataframe filters for each node
    temp_df = cat_filter(df, 'Gamut_Node_ID', node)  
    temp_df = temp_df.dropna(axis=0, subset=['Gamut ALL Values'])
    attributes = temp_df['Gamut_Attribute_Name'].unique()
    #create a "combined" column that includes all values, attribute names, and attribute definitions to define the corpus
    cols = ['Gamut ALL Values', 'Gamut_Attribute_Name', 'Gamut_Attribute_Definition']
    temp_df['combined'] = temp_df[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    #treat each attribute as a separate document and build a corpus
        
    for att in attributes:
        #create a second temp dataframe filtered at the attribute level
        temp2_df = cat_filter(temp_df, 'Gamut_Attribute_Name', att)
        temp2_df = temp2_df.drop_duplicates(subset=['Gamut_Attribute_Name'])
        #create a document out of each attribute (combo of name, definitions, and values)
        doc = temp2_df['combined'].str.cat(sep=' ')
        #process doc into tokens, remove punctuation and stopwords, lemmatize, and remove 'nans'
        words = get_words(doc)
        clean_words = [x for x in words if str(x) != 'nan']
        if len(clean_words) > 0:
            corpus.append(clean_words)
        corp_dict[att] = clean_words
        gamut_term_dict[att] = create_term_dict(clean_words)
            
    return corpus, gamut_term_dict


def attribute_name_match(df):
    grainger_words = dict()
    grainger_att_words = dict()
    gamut_words = dict()
    gamut_att_words = dict()
    grainger_word_dict = dict()
    
    grainger_nodes = df['Category_ID'].unique()
    gamut_nodes = df['Gamut_Node_ID'].unique()
    
    #build the grainger corpus dictionary (unique for each node)
    for node in grainger_nodes:
        #store cleaned corpus for each node in dictionary for future comparision
        print(node)
        grainger_words[node], grainger_att_words[node] = grainger_corp(df, node)
        print(grainger_att_words[node])
     #   freq_grainger = tf_idf(grainger_words[node])
     #   grainger_word_dict, grainger_word_list = vocab(freq_grainger)
     #   grainger_TDM = doc_matrix(freq_grainger, grainger_word_list, grainger_word_dict)
        grainger_word_dict[node] = word_counts(grainger_att_words[node])
        
     #   print("Grainger dataset has:\n%u unique words\n%u documents"%(grainger_TDM.shape))

    for node in gamut_nodes:
        #store cleaned corpus for each node in dictionary for future comparision
        print(node)
        gamut_words[node], gamut_att_words[node] = gamut_corp(df, node)
    #    freq_gamut = tf_idf(gamut_words[node])
    #    gamut_word_dict, gamut_word_list = vocab(freq_gamut)
    #    gamut_TDM = doc_matrix(freq_gamut, gamut_word_list, gamut_word_dict)
        gamut_word_dict[node] = word_counts(gamut_att_words[node])
    #    print("Gamut dataset has:\n%u unique words\n%u documents"%(gamut_TDM.shape))
        
    return grainger_words, gamut_att_words, grainger_word_dict, \
            gamut_words, gamut_att_words, gamut_word_dict #gamut_TDM, gamut_word_dict
# grainger_TDM, grainger_word_dict, 



attribute_df = pd.read_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test_27204.csv')

grainger_words = dict()
gamut_words = dict()
corpus = []

grainger_words, grainger_att_words, grainger_word_dict, \
gamut_words, gamut_att_words, gamut_word_dict = attribute_name_match(attribute_df)