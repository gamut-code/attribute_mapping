# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:40:34 2019

@author: xcxg109
"""
import pandas as pd
import numpy as np
import requests
import string
import re
from collections import defaultdict
from grainger_query import GraingerQuery
from queries_NUMERIC import gws_attr_query, gws_attr_values, grainger_attr_ETL_query
import data_process as process
import query_code_NUMERIC as q
import file_data_GWS as fd
from typing import Dict
import settings_NUMERIC as settings
import time
import memory_clear as mem

pd.options.mode.chained_assignment = None

gcom = GraingerQuery()


def match_category(df):
    """compare data colected from matching file (match_df) with grainger and gws data pulls and create a column to tell analysts
    whether attributes from the two systems have been matched"""
    
    df['Matching'] = 'no'
    
    for row in df.itertuples():
        grainger_string = str(row.Grainger_Attribute_Name)
        gws_string = str(row.Gamut_Attribute_Name)
        
#        gws_string = str(row.GWS_Attribute_Name)
        
        if (grainger_string) == (gws_string):
            df.at[row.Index,'Matching'] = 'Match'
        elif (grainger_string) in (gws_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif (gws_string) in (grainger_string):
            df.at[row.Index,'Matching'] = 'Potential Match'
        elif process.isBlank(row.Grainger_Attribute_Name) == False:
#            if process.isBlank(row.GWS_Attribute_Name) == True:
            
            if process.isBlank(row.Gamut_Attribute_Name) == True:
                df.at[row.Index,'Matching'] = 'Grainger only'
        elif process.isBlank(row.Grainger_Attribute_Name) == True:
#            if process.isBlank(row.GWS_Attribute_Name) == False:
            
            if process.isBlank(row.Gamut_Attribute_Name) == False:
                df.at[row.Index,'Matching'] = 'GWS only'
            
    return df


def choose_definition(df):
    """pick the definition to upload to GWS based on 1. Cat specific, 2. Attribute level, 3. old Gamut definition"""
    
    df['Definition'] = ''
    df['Definition Source'] = ''
    
    for row in df.itertuples():
        cat_def = str(row.Grainger_Category_Specific_Definition)
        attr_def = str(row.Grainger_Attribute_Definition)
        gamut_def = str(row.Gamut_Attribute_Definition)
        
        if process.isBlank(row.Grainger_Category_Specific_Definition) == False:
            df.at[row.Index,'Definition'] = cat_def
            df.at[row.Index,'Definition Source'] = 'Grainger Category Specific'
            
        elif process.isBlank(row.Grainger_Attribute_Definition) == False:
            df.at[row.Index,'Definition'] = attr_def
            df.at[row.Index,'Definition Source'] = 'Grainger Attribute Definition'
            
        elif process.isBlank(row.Gamut_Attribute_Definition) == False:
            df.at[row.Index,'Definition'] = gamut_def
            df.at[row.Index,'Definition Source'] = 'Gamut Definition'
            
    if df.empty == False:
        df = df.drop(['Grainger_Attribute_Definition', 'Grainger_Category_Specific_Definition', 'Gamut_Attribute_Definition'], axis=1) #remove unneeded columns

    return df


def split_value(df):
    """ split values into numerators + UOMs and create separate columns for each"""
    all_vals = pd.DataFrame()
    atts = df['Grainger_Attribute_Name'].unique()

    for attribute in atts:
        temp_df = df.loc[df['Grainger_Attribute_Name']== attribute]
        temp_df['Numeric'] = ""
        temp_df['String'] = ""

        for row in temp_df.itertuples():
            value = str(row.Grainger_Attribute_Value)

            r = re.compile('^\d*[\.\/]?\d*')

            temp_df.at[row.Index, 'Numeric'], temp_df.at[row.Index, 'String'] = re.split(r, value)
            num = r.search(value)           
            temp_df.at[row.Index, 'Numeric'] = num.group()

            temp_df['String'] = temp_df['String'].str.strip()

        temp_df['Count'] =1

        text_vals = pd.DataFrame(temp_df.groupby(['String'])['Count'].sum())
        text_vals = text_vals.reset_index()
        text_vals = text_vals.sort_values(by=['Count'], ascending=[False])
        text_vals['tex_val'] = ''

        for row in text_vals.itertuples():
            count = str(row.Count)
            value = str(row.String)
            tex_val = value + '[' + count + ']'
            text_vals.at[row.Index,'tex_val'] = tex_val

        temp_df['String Values (for Number Data Type)'] = '; '.join(item for item in text_vals['tex_val'] if item)

        all_vals = pd.concat([all_vals, temp_df], axis=0)

    return all_vals

    
def get_data_type(df, attribute):
    """using 'Numeric' and 'String' column values, determine which attributes are recommended as numeric, text, or range"""
    row_count = df['Grainger_Attribute_Value'].count()
    df['exclude'] = ''
    df['range'] = ''
    
    # Get a bool series representing positive 'Num' rows
    seriesObj = df.apply(lambda x: True if x['Numeric'] != "" else False , axis=1) 
    
    # Count number of True in series
    num_count = len(seriesObj[seriesObj == True].index)
    percent = num_count/row_count*100

    # build a list of items that are exluded as potential UOM values
    # if found, put values in a separate column used for evaluating 'Candidate' below
    value_exclusions = ['NEF', 'NPT', 'NPS', 'UNEF', 'Steel']        
        
    df['exclude'] = df['String'].apply(lambda x: ','.join([i for i in value_exclusions if i in x]))    
    excludeObj = df.apply(lambda x: True if x['exclude'] != "" else False , axis=1)
    exclude_count = len(excludeObj[excludeObj == True].index)
    exclude_percent = exclude_count/row_count*100

    # search for " to " in potential UOM values to detect range attributes
    range_tag = [' to ', 'to ', ' x ']

    df['range'] = df['String'].apply(lambda x: ','.join([i for i in range_tag if i in x]))
    rangeObj = df.apply(lambda x: True if x['range'] != "" else False , axis=1)
    range_count = len(rangeObj[rangeObj == True].index)
    range_percent = range_count/row_count*100
 
    # build a list of attributes that should automatically be considered "text"
    att_name = df['Grainger_Attribute_Name'].unique()
    att_name = att_name[0]

    # if 'merchandising attribute' is present in definition, attribute is also considered text    
    att_cat_def = df['Grainger_Category_Specific_Definition'].unique()
    att_cat_def = att_cat_def[0]
    att_cat_def = str(att_cat_def)
    
    att_def = df['Grainger_Attribute_Definition'].unique()
    att_def = att_def[0]
    att_def = str(att_def)
    
    evaluated = 'n'
    
    name_exclusions = ['Thread Size', 'Thread Depth', 'Item', 'For Use With', 'Connection', \
                       'Material', 'Type', 'Features', 'Finish', 'Includes']
    
    # check against the exclusion list first and mark them as already processed if we get a hit
    for name in name_exclusions:
        if name in att_name:
            evaluated = 'y'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'text'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'String Values (for Number Data Type)'] = '' 

    # if no exclusions are hit, do the other checks
    if evaluated == 'n':
        if 'merchandising' in att_cat_def or 'merchandising' in att_def:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'merchandising attribute' 
            df.loc[df['Grainger_Attr_ID'] == attribute, 'String Values (for Number Data Type)'] = '' 

        elif range_percent > 70:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'combination - text'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = '' 
#            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = float(percent)
#            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'].map('{:,.2f}'.format)

        elif 'Range' in att_name:
            if range_percent > 50:
                df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'combination - text'
            
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = float(percent)

        elif exclude_percent > 80:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'text'        
            df.loc[df['Grainger_Attr_ID'] == attribute, 'String Values (for Number Data Type)'] = '' 

        elif range_percent > 0 and range_percent < 70:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'potential number; contains {} comma separated attributes'.format(range_count)
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = float(percent)
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'].map('{:,.2f}'.format)

        elif percent <= 50:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'text'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'String Values (for Number Data Type)'] = '' 

        elif percent >= 51 and percent < 100:           
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'potential number'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = float(percent)
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'].map('{:,.2f}'.format)

        elif percent == 100:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'number'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = float(percent)
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'].map('{:,.2f}'.format)

        if percent <= 10:
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Recommended_Data_Type'] = 'text'
            df.loc[df['Grainger_Attr_ID'] == attribute, 'Percent_Numeric'] = '' 
            df.loc[df['Grainger_Attr_ID'] == attribute, 'String Values (for Number Data Type)'] = '' 

#    filename = 'C:/Users/xcxg109/NonDriveFiles/temp_' + str(attribute) + '.csv' 
#    df.to_csv(filename)

    return df


def match_lovs(lov_df, attribute):
    """compare the 'Grainger_Attr_ID' column against our list of LOVs"""
    
    lov_list = list()
    values_list = list()
    
#    attr_id = str(attribute) + '_ATTR'
    attr_id = attribute
 
    lov_list = lov_df['AttributeID'].tolist()
    lov_list = set(lov_list)

    if attr_id in lov_list:
        temp_df = lov_df.loc[lov_df['AttributeID']== attr_id]
        values_list = temp_df['Value'].tolist()

    return values_list
  
    
def process_sample_vals(df, row, pot):
    """ clean up the sample values column """

    potential_list = list(pot.split(', '))
    
    sample_val = str(row.Sample_Values)
    LOV_val = str(row.Restricted_Attribute_Value_Domain)
    
    for uom in potential_list:
        if '"' in str(uom):
            sample_val = sample_val.replace('"', ' in')
            LOV_val = sample_val.replace('"', ' in')

        if 'in.' in str(uom):
            sample_val = sample_val.replace('in.', 'in')
            LOV_val = sample_val.replace('in.', 'in')

        if 'ft.' in str(uom):
            sample_val = sample_val.replace('ft.', 'ft')
            LOV_val = sample_val.replace('ft.', 'ft')
            
        if 'yd.' in str(uom):
            sample_val = sample_val.replace('yd.', 'yd')   
            LOV_val = sample_val.replace('yd.', 'yd')   
        
        if 'fl.' in str(uom):
            sample_val = sample_val.replace('fl.', 'fl')    
            LOV_val = sample_val.replace('fl.', 'fl')    
        
        if 'oz.' in str(uom):
            sample_val = sample_val.replace('oz.', 'oz')    
            LOV_val = sample_val.replace('oz.', 'oz')    
        
        if 'pt.' in str(uom):
            sample_val = sample_val.replace('pt.', 'pt')    
            LOV_val = sample_val.replace('pt.', 'pt')    

        if 'qt.' in str(uom):
            sample_val = sample_val.replace('qt.', 'qt')     
            LOV_val = sample_val.replace('qt.', 'qt')     

        if 'kg.' in str(uom):
            sample_val = sample_val.replace('kg.', 'kg')    
            LOV_val = sample_val.replace('kg.', 'kg')    
        
        if 'gal.' in str(uom):
            sample_val = sample_val.replace('gal.', 'gal') 
            LOV_val = sample_val.replace('gal.', 'gal') 
        
        if 'lb.' in str(uom):
            sample_val = sample_val.replace('lb.', 'lb')   
            LOV_val = sample_val.replace('lb.', 'lb')   
        
        if 'cu.' in str(uom):
            sample_val = sample_val.replace('cu.', 'cu')  
            LOV_val = sample_val.replace('cu.', 'cu')  
        
        if 'sq.' in str(uom):
            sample_val = sample_val.replace('sq.', 'sq')    
            LOV_val = sample_val.replace('sq.', 'sq')    

        if '° C' in str(uom):
            sample_val = sample_val.replace('° C', '°C')
            LOV_val = sample_val.replace('° C', '°C')

        if '° F' in str(uom):
            sample_val = sample_val.replace('° F', '°F')     
            LOV_val = sample_val.replace('° F', '°F')     
        
        if 'deg.' in str(uom):        
            sample_val = sample_val.replace('deg.', '°')        
            LOV_val = sample_val.replace('deg.', '°')        

        if 'ga.' in str(uom):        
            sample_val = sample_val.replace('ga.', 'ga')        
            LOV_val = sample_val.replace('ga.', 'ga')        

        if 'point' in str(uom):
            sample_val = sample_val.replace('point', 'pt.')        
            LOV_val = sample_val.replace('point', 'pt.')        

        if 'min.' in str(uom):
            sample_val = sample_val.replace('min.', 'min')
            LOV_val = sample_val.replace('min.', 'min')

        if 'sec.' in str(uom):
            sample_val = sample_val.replace('sec.', 'sec')
            LOV_val = sample_val.replace('sec.', 'sec')

        if 'hr.' in str(uom):
            sample_val = sample_val.replace('hr.', 'hr')        
            LOV_val = sample_val.replace('hr.', 'hr')        

        if 'wk.' in str(uom):
            sample_val = sample_val.replace('wk.', 'wk') 
            LOV_val = sample_val.replace('wk.', 'wk') 

        if 'mo.' in str(uom):
            sample_val = sample_val.replace('mo.', 'mo')
            LOV_val = sample_val.replace('mo.', 'mo')

        if 'µ' in str(uom):
            sample_val = sample_val.replace('µ', 'u')        
            LOV_val = sample_val.replace('µ', 'u')        

    df.at[row.Index,'Sample_Values'] = sample_val
    df.at[row.Index,'Restricted_Attribute_Value_Domain'] = LOV_val

    return df 
   
    
def determine_uoms(df, uom_df, values_list):
    """for all non 'text' data types, compare 'String' field to our UOM list, then compare these potential UOMs
    to current GWS UOM groupings. finally, determine whether numeric part of the value is a fraction or decimal"""

    unit_df = pd.DataFrame()
    text_list = list()
    text_list_lower = list()
    name_list = list()
    name_list_lower = list()
    potential_list = list()
    best_potential_list = list()
    second_pot_list = list()
    last_chance_list = list()
    match_name_list = list()
    second_pot_name_list = list()
    uom_list = list()
    unit_group_name = list()
    uom_ids =  list()
    matched_ids = list()
    intersect_list = list()
    unit_names = list()
    uom_dict = defaultdict(dict)
    dict_list = list()
    all_uom_names = list()
    dict_names = list() 

    # build unique UOM list for comparison
    uom_list = uom_df['unit_name'].tolist()
    uom_list = set(uom_list)

    all_uom_names = uom_df['unit_group_name'].tolist()
    
    for i in range(len(all_uom_names)):
        all_uom_names[i] = all_uom_names[i].lower()

    all_uom_names = set(all_uom_names)

    data_type = df['Recommended_Data_Type'].unique()
    
    if data_type != 'text':
        # consider attribute name field as a sourse of potential uoms
        # evalulate lower case versions of attribute names also, to look for matches like "PSI"
        name_value = df['Grainger_Attribute_Name'].unique()
        name_value = name_value[0]
        # remote "to" and punctuation before evaluating attribute names
        name_value = name_value.replace('to','')
        name_value = name_value.replace('(','')
        name_value = name_value.replace(')','')
        name_value = name_value.replace('.','')

        name_value_lower = name_value.lower()

        name_list.append(name_value)
        name_list_lower.append(name_value_lower)
        
        name_match = set(name_list).intersection(set(uom_list))
            
        if not name_match:
            name_match = set(name_list_lower).intersection(set(uom_list))

        if not name_match:
            pot_name_uom = [x for x in uom_list if x in name_value.split()]

            if not pot_name_uom:
                pot_name_uom = [x for x in uom_list if x in name_value_lower.split()]

        # create list of potential UOMs for the attribute
        if name_match:
            match_name_list.extend(name_match)
        elif pot_name_uom:
            second_pot_name_list.extend(pot_name_uom)

    # now search through rows of the df (NOTE: df passed here is only for a single attribute) and look at individual values
    for row in df.itertuples():
        # for non text fields, run a search for potential UOM groups and categorize
        data_type = df.at[row.Index,'Recommended_Data_Type']

        if data_type != 'text':

            # evaluate the text portion of attribute value field
            str_value = df.at[row.Index,'String']
            str_value = str(str_value)
            str_value = str_value.replace('to',' ')
            str_value = str_value.replace('-',' ')

            if ' x ' in str_value:
                str_value = str_value.replace(' x ',' ')
                str_value = str_value.replace(' L', ' ')
                str_value = str_value.replace(' H', ' ')
                str_value = str_value.replace(' W', ' ')

            str_value_lower = str_value.lower()
            
            # force 'String' content into a list so we can evaulate the entire string for a match against uom_list
            text_list.append(str_value)
            text_list_lower.append(str_value_lower)
            
            # if 'String' field contains value(s), compare to UOM list and assigned to 'Potential UOMs'
            if str_value != '':
                # check for a match of the entire contant of 'String' against our uom_list
                match = set(text_list).intersection(set(uom_list))
                # but if we don't find an exact match, parse 'String' content and attempt to match up with uom_List

                if not match:
                    match = set(text_list_lower).intersection(set(uom_list))
                    
                if not match:
                    pot_uom = [x for x in uom_list if x in str_value.split()]
                    # if parse by word match still fails, try one more time at a more granular level for a match                    

                    if not pot_uom:
                        pot_uom = [x for x in uom_list if x in str_value_lower.split()]

                    if not pot_uom:
                        last_chance = [x for x in uom_list if x in str_value]

                        if '"' in last_chance:
                            last_chance = '"'
                        else:
                            last_chance = ''
                            
                # create list of potential UOMs for the attribute
                if match:
                    best_potential_list.extend(match)
                elif pot_uom:
                    second_pot_list.extend(pot_uom)
                elif last_chance:
                    last_chance_list.extend(last_chance)

            # evaluate whether 'Numeric' value can be classified as decimal or fraction
            num = df.at[row.Index,'Numeric']
            num = str(num)
        
            if num != '':
                if '.' in num:
                    df.at[row.Index,'Numeric Display Type'] = 'decimal'
                elif '/' in num:
                    df.at[row.Index,'Numeric Display Type'] = 'fraction'
                else:
                    df.at[row.Index,'Numeric Display Type'] = 'decimal'
                
    if best_potential_list:
        potential_list = set(best_potential_list)
    elif second_pot_list:
        potential_list = set(second_pot_list)
    elif match_name_list:
        potential_list = set(match_name_list)
    elif second_pot_name_list:
        potential_list = set(second_pot_name_list)
    elif last_chance_list:
        potential_list = set(last_chance_list)
        
    for unit in potential_list:
        temp_df = uom_df.loc[uom_df['unit_name']== unit]
            
        # create a pool of all ids that contain the specific UOM
        matched_ids = temp_df['unit_group_id'].tolist()
        matched_ids = [int(x) for x in matched_ids if ~np.isnan(x)]

        for match in matched_ids:
            temp_uom =   uom_df.loc[uom_df['unit_group_id']== match]
            unit_names = temp_uom['unit_name'].tolist()
            unit_group_name = temp_uom['unit_group_name'].tolist()
            unit_group_name = set(unit_group_name)
                
            intersect_list = set(potential_list).intersection(set(unit_names))
            match_percent = len(intersect_list)/len(potential_list)*100
            match_percent = round(match_percent, 2)

            # create dictionary entry for each matched uom + match percentage                
            if match not in uom_dict:
                uom_dict[match]['name'] = unit_group_name
                uom_dict[match]['percent'] = match_percent 
                 
        temp_df = temp_df[['unit_group_id', 'unit_group_name', 'unit_name']]
        unit_df = pd.concat([unit_df, temp_df], axis=0)

    df = df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
        
    if unit_df.empty == False:
        unit_df = unit_df.drop_duplicates(subset=['unit_group_id'])  #group by Category_ID and attribute name and keep unique

        uom_ids = unit_df['unit_group_id'].tolist()
        uom_ids = [int(x) for x in uom_ids if ~np.isnan(x)]

        unit_group_name = unit_df['unit_group_name'].tolist()

        if uom_dict:
            # sort the dict for highest matching potential
            # if only one match = 100% that's what we'll use
            uom_sorted = sorted(uom_dict.items(), key=lambda item: int(item[1]['percent']), reverse=True)
                    
            for key, value in uom_sorted:
                kA = key
                vA = value['percent']
                
                nA = str(value['name'])
                nA = nA.translate(str.maketrans('', '', string.punctuation))
                nA = re.sub(' +', ' ', nA)                
                elementA = '|' + str(kA) + '- ' + str(nA) + ' : ' + str(vA) + '|'

                dict_list.append(elementA)
                dict_names.append(nA)
            
            dict_names = set(dict_names)
 
    for row in df.itertuples():
        # if f and/or UOM lists are populated, write them to the df
        if values_list:
            values_list = '; '.join(str(x) for x in values_list)
            df.at[row.Index, 'Restricted_Attribute_Value_Domain'] = values_list

        if potential_list:
            df.at[row.Index, 'Potential_UOMs'] = potential_list
            
        if uom_ids:
            if len(uom_ids) == 1:
                single_id = uom_ids.pop()
                df.at[row.Index,'Recommended Unit of Measure ID'] = single_id
                # if there is only one, add it to the 'real' UOM column
                df.at[row.Index,'Unit of Measure Domain (Group ID)'] = single_id
                                
            else:
                for name in name_list_lower:		
                    name = name.translate(str.maketrans('', '', string.punctuation))
                    name = re.sub(' +', ' ', name)
                    name_list = name.split(' ')

                    for n in name_list:
                        matching = [d for d in all_uom_names if n in d]		

                        if matching:
                            match = matching.pop()
            
                            entry = {k: v for k, v in uom_dict.items() if v['name'] == match}

                            for dic in entry:
                                dic_id = dic
                                nB = uom_dict[dic_id]['name']
                                vB = uom_dict[dic_id]['percent']
                                elementB = '|' + str(dic) + ' : ' + str(nB) + ' = ' + str(vB) + ' |'

                                if vB == 100:
#                                    df.at[row.Index,'Unit of Measure Group Name'] = nB
                                    df.at[row.Index,'Recommended Unit of Measure ID'] = elementB

                if df.at[row.Index,'Recommended Unit of Measure ID'] == '':
                    if dict_list:
                        df.at[row.Index,'Recommended Unit of Measure ID'] = dict_list
                    else:
                        df.at[row.Index,'Recommended Unit of Measure ID'] = uom_ids			
                        
#            if df.at[row.Index,'Unit of Measure Group Name'] == '':
#                if dict_names:
#                    df.at[row.Index,'Unit of Measure Group Name'] = dict_names
#                else:
#                    df.at[row.Index,'Unit of Measure Group Name'] = unit_group_name
                    
    return df

    
def analyze(df, uom_df, lov_df):
    """use the split fields in grainger_df to analyze suitability for number conversion and included in summary df"""
    analyze_df = pd.DataFrame()
    
    # create the numeric/string columns
    df = split_value(df)

    atts = df['Grainger_Attr_ID'].unique()

    df['Percent_Numeric'] = ''
    df['Potential_UOMs'] = ''
    df['Unit of Measure Domain'] = ''
#    df['Unit of Measure Group Name'] = ''
    df['Restricted_Attribute_Value_Domain'] = ''
    df['Numeric display type'] = ''
    df['Recommended Unit of Measure ID'] = ''
    df['Recommended_Data_Type'] = ''
    
    for attribute in atts:
        temp_df = df.loc[df['Grainger_Attr_ID']== attribute]
        temp_df = get_data_type(temp_df, attribute)
        values_list = match_lovs(lov_df, attribute)
        temp_df = determine_uoms(temp_df, uom_df, values_list)

        analyze_df = pd.concat([analyze_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df
        
#    analyze_df.to_csv('F:/CGabriel/Grainger_Shorties/OUTPUT/test.csv')
    return analyze_df


def gamut_process(gnode, gamut_dict):
    """if gamut node has not been preiously process (in gamut_dict), process and add it to the dictionary"""
    print('gnode = ', gnode)
    gamut_df = q.gamut_definition(gnode, 'tax_att."categoryId"')
        
    if gamut_df.empty==False:
        gamut_df['alt_gamut_name'] = process.process_att(gamut_df['Gamut_Attribute_Name'])  #prep att name for merge
        gamut_dict[gnode] = gamut_df #store the processed df in dict for future reference

    else:
        print('{} EMPTY DATAFRAME'.format(gnode))    
        
    return gamut_dict, gamut_df

    
def gws_process(gws_node, gws_dict: Dict):
    """if gws node has not been previously processed (in gws_dict), process and add it to the dictionary"""
    gws_df = q.gws_atts(gws_attr_query, gws_node, 'tax.id')  #tprod."categoryId"')  #get gws attribute values for each gamut_l3 node\
    
    if gws_df.empty==False:
        gws_df = gws_df.drop_duplicates(subset='GWS_Attr_ID')  #gws attribute IDs are unique, so no need to group by pim node before getting unique
        gws_df['alt_gws_name'] = process.process_att(gws_df['GWS_Attribute_Name'])  #prep att name for merge
        gws_dict[gws_node] = gws_df #store the processed df in dict for future reference

    else:
        print('{} EMPTY DATAFRAME'.format(gws_node))    
        
    return gws_dict, gws_df


def grainger_process(grainger_df, grainger_all, uom_df, lov_df, gamut_dict, grainger_node):
    """create a list of grainger skus, run through through the gws_skus query and pull gws attribute data if skus are present
        concat both dataframs and join them on matching attribute names"""
    
    print('grainger node = ', grainger_node)
    df = pd.DataFrame()
        
    cat_name = grainger_df['Category_Name'].unique()
    cat_name = list(cat_name)
    cat_name = cat_name.pop()
    print('cat name = {} {}'.format(grainger_node, cat_name))

    grainger_skus = grainger_df.drop_duplicates(subset='Grainger_SKU')  #create list of unique grainger skus that feed into gws query
    grainger_sku_count = len(grainger_skus)
    print('grainger sku count = ', grainger_sku_count)
    
    grainger_df = analyze(grainger_df, uom_df, lov_df)

    grainger_df = grainger_df.drop_duplicates(subset=['Category_ID', 'Grainger_Attr_ID'])  #group by Category_ID and attribute name and keep unique
    grainger_df['STEP Blue Path'] = grainger_df['Segment_Name'] + ' > ' + grainger_df['Family_Name'] + \
                                                        ' > ' + grainger_df['Category_Name']

    grainger_df = grainger_df.drop(['Grainger_SKU', 'Grainger_Attribute_Value'], axis=1) #remove unneeded columns    
    grainger_df = pd.merge(grainger_df, grainger_all, on=['Grainger_Attr_ID'])
    
    # for non-text rows, clean up UOMs in sample value column
    for row in grainger_df.itertuples():
        potential_uoms = str(row.Potential_UOMs)
        dt = str(row.Recommended_Data_Type)

        if dt != 'text':
            grainger_df = process_sample_vals(grainger_df, row, potential_uoms)
    
    grainger_df['alt_grainger_name'] = process.process_att(grainger_df['Grainger_Attribute_Name'])  #prep att name for merge

    gamut_skus = q.gws_skus(grainger_skus) #get gamut sku list to determine pim nodes to pull

    # if gws skus are present, go get the gamut attribute definition for the node
    if gamut_skus.empty==False:
       gamut_l3 = gamut_skus['Gamut_Node_ID'].unique()

       print('gamut L3s ', gamut_l3)
       
       for gamut_node in gamut_l3:
           if gamut_node in gamut_dict:
               gamut_df = gamut_dict[gamut_node]
               print ('node {} in Gamut dict'.format(gamut_node))
           else:
              gamut_dict, gamut_df = gamut_process(gamut_node, gamut_dict)

           if gamut_df.empty==False:
                node_name = gamut_df['Gamut_Node_Name'].unique()
                node_name = list(node_name)
                node_name = node_name.pop()
                print('node name = {} {}'.format(gamut_node, node_name))
                
                #add correlating grainger and gamut data to opposite dataframes
                grainger_df = q.grainger_assign_nodes(grainger_df, gamut_df)
                gamut_df = q.gamut_assign_nodes(grainger_df, gamut_df)
                
                temp_df = pd.merge(grainger_df, gamut_df, left_on=['alt_grainger_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                                   'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                                   'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
                                                right_on=['alt_gamut_name', 'Category_ID', 'Gamut_Node_ID', 'Gamut_Category_ID', \
                                                          'Gamut_Category_Name', 'Gamut_Node_Name', 'Gamut_PIM_Path', 'STEP Blue Path', \
                                                          'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

                temp_df = match_category(temp_df) #compare grainger and gamut atts and create column to say whether they match 

                df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gamut node to the final df
                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')
                df = df[df.Matching != 'GWS only']
       #         temp_df.to_csv('C:/Users/xcxg109/NonDriveFiles/graingerProcessDF.csv')            
           else:
                print('GWS Node {} EMPTY DATAFRAME'.format(gamut_node))

    else:
        df = grainger_df
        df['Gamut_Attribute_Definition'] = ''
        
        print('No Gamut SKUs for Grainger node {}'.format(level_1))

#                df = pd.merge(df, gamut_df, left_on=['alt_grainger_name'], \
 #                                               right_on=['alt_gamut_name'], how='outer')

  #              df = match_category(df) #compare grainger and gws atts and create column to say whether they match 
  #              df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')
                

#    if gws_skus.empty==False:
        #create a dictionary of the unique gws nodes that corresponde to the grainger node 
 #       gws_l3 = gws_skus['GWS_Node_ID'].unique()  #create list of pim nodes to pull
 #       print('GWS L3s ', gws_l3)
        
 #       for node in gws_l3:
 #           if node in gws_dict:
 #               gws_df = gws_dict[node]
 #               print ('node {} in GWS dict'.format(node))
                
 #           else:
  #              gws_dict, gws_df = gws_process(node, gws_dict)

  #          if gws_df.empty==False:
 #               node_name = gws_df['GWS_Node_Name'].unique()
  #              node_name = list(node_name)
   #             node_name = node_name.pop()
    #            print('node name = {} {}'.format(node, node_name))
                #add correlating grainger and gws data to opposite dataframes
   #             grainger_df = q.grainger_assign_nodes(grainger_df, gws_df)
   #             gws_df = q.gws_assign_nodes(grainger_df, gws_df)
 
     #           temp_df = pd.merge(grainger_df, gws_df, left_on=['alt_grainger_name', 'Category_ID', 'GWS_Node_ID', 'GWS_Category_ID', \
      #                                                             'GWS_Category_Name', 'GWS_Node_Name', 'GWS_PIM_Path', 'STEP Blue Path', \
       ##                                                            'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], 
         #                                       right_on=['alt_gws_name', 'Category_ID', 'GWS_Node_ID', 'GWS_Category_ID', \
          #                                                'GWS_Category_Name', 'GWS_Node_Name', 'GWS_PIM_Path', 'STEP Blue Path', \
           #                                               'Segment_ID', 'Segment_Name', 'Family_ID', 'Family_Name', 'Category_Name'], how='outer')

#                temp_df = match_category(temp_df) #compare grainger and gws atts and create column to say whether they match 

            #    df = pd.concat([df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df
#                df['Matching'] = df['Matching'].str.replace('no', 'Potential Match')

                # drop all of the rows that are 'GWS only' in the Match column
#                df = df[df.Matching != 'GWS only']

#                df = df.drop(['alt_grainger_name', 'GWS_Node_ID', 'GWS_Category_ID', 'GWS_Category_Name', \
#                              'GWS_Node_Name', 'GWS_PIM_Path'], axis=1)        

            #else:
             #   print('GWS Node {} EMPTY DATAFRAME'.format(node))

#    else:
 #       df = grainger_df
  #      print('No GWS SKUs for Grainger node {}'.format(grainger_node))

    df.reset_index(drop=True, inplace=True)
    df = choose_definition(df)

    return df, gamut_dict #where gamut_att_temp is the list of all normalized values for gamut attributes


def attribute_process(grainger_df, uom_df, lov_df, gamut_dict: Dict, att_process_node):
    attribute_df = pd.DataFrame()
    grainger_att_vals = pd.DataFrame()

    # remove "-" values from attribute value field
    grainger_df.loc[grainger_df.Grainger_Attribute_Value == '-', 'Grainger_Attribute_Value'] = np.NaN
   
    grainger_att_vals = q.grainger_values(grainger_df)

    temp_df, gamut_dict = grainger_process(grainger_df, grainger_att_vals, uom_df, lov_df, gamut_dict, att_process_node)
    attribute_df = pd.concat([attribute_df, temp_df], axis=0, sort=False)
    print ('Grainger node = {}\n'.format(att_process_node))

    attribute_df = attribute_df.drop_duplicates(subset=['Grainger_Attr_ID'])
            
    attribute_df = attribute_df.rename(columns={'Segment_ID':'Segment ID', 'Segment_Name':'Segment Name', \
                'Family_ID':'Family ID', 'Family_Name':'Family Name', 'Category_ID':'STEP Category ID', \
                'Category_Name':'Category Name', 'Grainger_Attr_ID':'STEP Attribute ID', 'Percent_Numeric':'%_Numeric', \
                'Potential_UOMs':'Potential UOMs', 'Grainger_Attribute_Name':'Attribute Name', \
                'Recommended_Data_Type':'Recommended Data Type', 'Sample_Values':'Sample Values', \
                'Restricted_Attribute_Value_Domain':'Restricted Attribute Value Domain'})
    
    return attribute_df, gamut_dict


df_upload = pd.DataFrame()
gws_dict = dict()
gamut_dict = dict()
    
start_time = time.time()

search_level = 'cat.CATEGORY_ID'    # l3 is default search level

# read in uom and LOV files
uom_df = pd.DataFrame()

#uom_groups_url = 'https://raw.githubusercontent.com/gamut-code/attribute_mapping/master/UOM_data_sheet.csv'
# create df of the uom groupings (ID and UOMs for each group)
#data_file = requests.get(uom_groups_url).content
#uom_df = pd.read_csv(io.StringIO(data_file.decode('utf-8')))

# get uom list
filename = 'C:/Users/xcxg109/NonDriveFiles/reference/UOM_data_sheet.csv'
uom_df = pd.read_csv(filename)
# create df of the lovs and their concat values
lov_df = q.get_LOVs()

data_type = fd.search_type()

print('working...')

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()

    search_data = fd.data_in(data_type, settings.directory_name)

    if search_level == 'cat.CATEGORY_ID':
        for level_1 in search_data:
            grainger_df = q.gcom.grainger_q(grainger_attr_ETL_query, search_level, level_1)
            print('k = ', level_1)
            if grainger_df.empty == False:
                df_upload, gamut_dict = attribute_process(grainger_df, uom_df, lov_df, gamut_dict, level_1)
            else:
                print('{} No attribute data'.format(level_1))
                    
            if df_upload.empty==False:
                fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
               
            else:
                print('EMPTY DATAFRAME')
                    
            print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))

    else:
        print ('search level = ', search_level)
        
        for level_1 in search_data:
            print('** K = ', level_1)
            
            grainger_skus = q.grainger_nodes(level_1, search_level)
            grainger_l3 = grainger_skus['Category_ID'].unique()  #create list of pim nodes to pull
            
            print('\ngrainger L3s = ', grainger_l3)
            
            for l3 in grainger_l3:
                print('\nL3 = ', l3)
                grainger_df = q.gcom.grainger_q(grainger_attr_ETL_query, 'cat.CATEGORY_ID', l3)

                if grainger_df.empty == False:
                    temp_df_upload, gamut_dict = attribute_process(grainger_df, uom_df, lov_df, gamut_dict, l3)            
                    df_upload = pd.concat([df_upload, temp_df_upload], axis=0, sort=False)
                    
                else:
                    print('{} No attribute data'.format(level_1))
                    
            if df_upload.empty==False:
                fd.GWS_upload_data_out(settings.directory_name, df_upload, search_level)
                
            print('end of K = ', level_1)
            
            filename = 'C:/Users/xcxg109/NonDriveFiles/backup_'+str(level_1)+'.csv'
            # export to CSV as backup in case ExcelWriter fails
            df_upload.to_csv(filename)
               
        else:
            print('EMPTY DATAFRAME')
        
            print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
    
 #   mem.clear_all()
                    
print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))



