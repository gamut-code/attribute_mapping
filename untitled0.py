# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 17:12:17 2020

@author: xcxg109
"""

import pandas as pd
import WS_query_code as q




def match_lovs(lov_df, lov_list, attr_id):
    """compare the 'Grainger_Attr_ID' column against our list of LOVs"""
    
    values_list = list()

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
            LOV_val = LOV_val.replace('"', ' in')

        if 'in.' in str(uom):
            sample_val = sample_val.replace('in.', 'in')
            LOV_val = LOV_val.replace('in.', 'in')

        if 'ft.' in str(uom):
            sample_val = sample_val.replace('ft.', 'ft')
            LOV_val = LOV_val.replace('ft.', 'ft')
            
        if 'yd.' in str(uom):
            sample_val = sample_val.replace('yd.', 'yd')   
            LOV_val = LOV_val.replace('yd.', 'yd')   
        
        if 'fl.' in str(uom):
            sample_val = sample_val.replace('fl.', 'fl')    
            LOV_val = LOV_val.replace('fl.', 'fl')    
        
        if 'oz.' in str(uom):
            sample_val = sample_val.replace('oz.', 'oz')    
            LOV_val = LOV_val.replace('oz.', 'oz')    
        
        if 'pt.' in str(uom):
            sample_val = sample_val.replace('pt.', 'pt')    
            LOV_val = LOV_val.replace('pt.', 'pt')    

        if 'qt.' in str(uom):
            sample_val = sample_val.replace('qt.', 'qt')     
            LOV_val = LOV_val.replace('qt.', 'qt')     

        if 'kg.' in str(uom):
            sample_val = sample_val.replace('kg.', 'kg')    
            LOV_val = LOV_val.replace('kg.', 'kg')    
        
        if 'gal.' in str(uom):
            sample_val = sample_val.replace('gal.', 'gal') 
            LOV_val = LOV_val.replace('gal.', 'gal') 
        
        if 'lb.' in str(uom):
            sample_val = sample_val.replace('lb.', 'lb')   
            LOV_val = LOV_val.replace('lb.', 'lb')   
        
        if 'cu.' in str(uom):
            sample_val = sample_val.replace('cu.', 'cu')  
            LOV_val = LOV_val.replace('cu.', 'cu')  
        
        if 'sq.' in str(uom):
            sample_val = sample_val.replace('sq.', 'sq')    
            LOV_val = LOV_val.replace('sq.', 'sq')    

        if '° C' in str(uom):
            sample_val = sample_val.replace('° C', '°C')
            LOV_val = LOV_val.replace('° C', '°C')

        if '° F' in str(uom):
            sample_val = sample_val.replace('° F', '°F')     
            LOV_val = LOV_val.replace('° F', '°F')     
        
        if 'deg.' in str(uom):        
            sample_val = sample_val.replace('deg.', '°')        
            LOV_val = LOV_val.replace('deg.', '°')        

        if 'ga.' in str(uom):        
            sample_val = sample_val.replace('ga.', 'ga')        
            LOV_val = LOV_val.replace('ga.', 'ga')        

        if 'point' in str(uom):
            sample_val = sample_val.replace('point', 'pt.')        
            LOV_val = LOV_val.replace('point', 'pt.')        

        if 'min.' in str(uom):
            sample_val = sample_val.replace('min.', 'min')
            LOV_val = LOV_val.replace('min.', 'min')

        if 'sec.' in str(uom):
            sample_val = sample_val.replace('sec.', 'sec')
            LOV_val = LOV_val.replace('sec.', 'sec')

        if 'hr.' in str(uom):
            sample_val = sample_val.replace('hr.', 'hr')        
            LOV_val = LOV_val.replace('hr.', 'hr')        

        if 'wk.' in str(uom):
            sample_val = sample_val.replace('wk.', 'wk') 
            LOV_val = LOV_val.replace('wk.', 'wk') 

        if 'mo.' in str(uom):
            sample_val = sample_val.replace('mo.', 'mo')
            LOV_val = LOV_val.replace('mo.', 'mo')

        if 'µ' in str(uom):
            sample_val = sample_val.replace('µ', 'u')        
            LOV_val = LOV_val.replace('µ', 'u')        




# get uom list
filename = 'C:/Users/xcxg109/NonDriveFiles/reference/UOM_data_sheet.csv'
uom_df = pd.read_csv(filename)
# create df of the lovs and their concat values

filename = 'C:/Users/xcxg109/NonDriveFiles/reference/LOV_Categories.csv'
lov_df, lov_list = q.get_LOVs(filename)

lov_df.to_csv('C:/Users/xcxg109/NonDriveFiles/reference/test.csv')
