# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 16:04:58 2019

@author: xcxg109
"""

import string
from collections import Counter
import pandas as pd
import numpy as np
from spacy.lang.en import English

import re


def process_att(attribute):
    """text processing of attributes to facilitate matching"""
    attribute = attribute.str.lower()
 #   pat= re.compile(r"\.\(\d/\d\)$")
    pat = re.compile(r" \(..\.\)")
    attribute = attribute.str.replace(pat, "")
    attribute = attribute.str.replace('  (merch)', "")
    attribute = attribute.str.replace('  (MERCH)', "")
    attribute = attribute.str.replace('also known as', 'item')
    attribute = attribute.str.replace('standards', 'specifications met')

    attribute = attribute.str.replace('overall ', "")

    attribute= attribute.str.replace('dia\.', 'diameter')
    attribute = attribute.str.replace(r'\bi\.d\.\b', 'inner diameter')
    attribute = attribute.str.replace(r'\bid\b', 'inner diameter')
    attribute = attribute.str.replace(r'\bo\.d\.\b', 'outer diameter')
    attribute = attribute.str.replace(r'\bod\b', 'outer diameter')
    

    return attribute


def isBlank (myString):
    return (myString and pd.isnull(myString))

        
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

def grainger_corp(df, node):
    """build a unique document corpus for each grainger node, whith each attribute considered a document"""
    corpus = []
    corp_dict = dict()
    words = []
    clean_words = []
    
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
        
    return corpus, corp_dict


def gamut_corp(df, node):
    """build a unique document corpus for each grainger node, whith each attribute considered a document"""
    corpus = []
    corp_dict = dict()
    words = []
    clean_words = []
    
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
            
    return corpus, corp_dict


def inv_freq(corpus):
    docs = len(corpus)
    
    doc_count = {}

    for doc in corpus:
        word_set = set(doc)

        for word in word_set:
            doc_count[word] = doc_count.get(word, 0) + 1
    
    freq = {}
    
    #calculate log freq for how often a word occurs accross multiple docs (grainger/gamut attributes)
    for word in doc_count:
        freq[word] = np.log(docs/doc_count[word]) 
    
    return freq
           

def tf_idf(corpus):
    freq = inv_freq(corpus)
    
    doc_freq = []
    
    for doc in corpus:
        doc_freq.append(Counter(doc))
    
    for doc in doc_freq:
        for word in doc:
            doc[word] = doc[word]*freq[word]
            
    return doc_freq


def vocab(doc_freq):
    words = set()
    
    for doc in doc_freq:
        words |= doc.keys()
    
    word_list = list(words)
    word_dict = dict(zip(word_list, range(len(word_list))))
    
    return word_dict, word_list


def doc_matrix(doc_freq, word_list, word_dict):
    """create individual term document matrix for grainger and gamut nodes for comparison"""
    vocab = len(word_dict)
    doc_count = len(doc_freq)
    
    term_matrix = np.zeros((vocab, doc_count))
    
    for doc in range(doc_count):
        document = doc_freq[doc]
        
        for word in document.keys():
            position = word_dict[word]
            
            term_matrix[position, doc] = document[word]
            
    return term_matrix


def match_docs(text, TDM):
    new_vector = np.zeros(TDM.shape[1])
    
    for word in corpus:
        pos = word_dict[word]
        new_vector += TDM[pos, :]
        
    # Now the entries of new_vector tell us which documents are activated by this one.
    # Let's extract the list of documents sorted by activation
    doc_list = sorted(zip(range(TDM.shape[1]), new_vector), key=lambda x:x[1], reverse=True)
    
    return doc_list


def attribute_name_match(df):
    grainger_words = dict()
    grainger_att_words = dict()
    gamut_words = dict()
    gamut_att_words = dict()
    
    grainger_nodes = df['Category_ID'].unique()
    gamut_nodes = df['Gamut_Node_ID'].unique()
    
    #build the grainger corpus dictionary (unique for each node)
    for node in grainger_nodes:
        #store cleaned corpus for each node in dictionary for future comparision
        print(node)
        grainger_words[node], grainger_att_words[node] = grainger_corp(df, node)
        freq_grainger = tf_idf(grainger_words[node])
        grainger_word_dict, grainger_word_list = vocab(freq_grainger)
        grainger_TDM = doc_matrix(freq_grainger, grainger_word_list, grainger_word_dict)
        print("Grainger dataset has:\n%u unique words\n%u documents"%(grainger_TDM.shape))

    for node in gamut_nodes:
        #store cleaned corpus for each node in dictionary for future comparision
        print(node)
        gamut_words[node], gamut_att_words[node] = gamut_corp(df, node)
        freq_gamut = tf_idf(gamut_words[node])
        gamut_word_dict, gamut_word_list = vocab(freq_gamut)
        gamut_TDM = doc_matrix(freq_gamut, gamut_word_list, gamut_word_dict)
        print("Gamut dataset has:\n%u unique words\n%u documents"%(gamut_TDM.shape))
        
    return grainger_words, gamut_att_words, grainger_TDM, grainger_word_dict, \
            gamut_words, gamut_att_words, gamut_TDM, gamut_word_dict


#attribute_df = pd.read_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test_27204.csv')

#grainger_words = dict()
#gamut_words = dict()

#corpus = []

#grainger_words, grainger_att_words, grainger_TDM, grainger_word_dict, \
 #   gamut_words, gamut_att_words, gamut_TDM, gamut_word_dict = attribute_name_match(attribute_df)

#temp_df = cat_filter(attribute_df, 'Gamut_Node_ID', 1929)
#cols = ['Gamut ALL Values', 'Gamut_Attribute_Name', 'Gamut_Attribute_Definition']
#temp_df['combined'] = temp_df[cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)

#temp2_df = cat_filter(temp_df, 'Gamut_Attribute_Name', 'overall height')
#temp2_df = temp2_df.drop_duplicates(subset=['Gamut_Attribute_Name'])
#new_doc = temp2_df['combined'].str.cat(sep=' ')
#process doc into tokens, remove punctuation and stopwords, lemmatize, and remove 'nans'
#words = get_words(new_doc)
#clean_words = [x for x in words if str(x) != 'nan']
#if len(clean_words) > 0:
 #   corpus.append(clean_words)



#related = match_docs(corpus, grainger_TDM)
