# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 11:57:42 2021

@author: xcxg109
"""
import pandas as pd

def attr_values(df):
    all_vals = pd.DataFrame()
    
    func_df = df.copy()
    func_df['Count'] =1
    func_df['Comma Separated Values'] = ''
    print('func_df = ', func_df.columns)
    atts = func_df['WS_Attr_ID'].unique().tolist()
    
    vals = pd.DataFrame(func_df.groupby(['WS_Attr_ID', 'WS_Attribute_Name', 'Normalized_Value'])['Count'].sum())
    vals = vals.reset_index()
 
    for attribute in atts:
        temp_df_att = vals.loc[vals['WS_Attr_ID']== attribute]
        temp_df_att = temp_df_att.sort_values(by=['Count'], ascending=[False])

 #       temp_df_att.to_csv('C:/Users/xcxg109/NonDriveFiles/hoist.csv')    

        for row in temp_df_att.itertuples():
            val = str(temp_df_att.at[row.Index, 'Normalized_Value'])
            ct = str(temp_df_att.at[row.Index, 'Count'])

            val_count = val + '[' + ct + ']'

            temp_df_att.at[row.Index, 'val_counts'] = val_count

#        temp_df_att = temp_df_att.drop_duplicates(subset =['WS_Attr_ID'])
#        temp_df_att['WS_Attr_ID'] = str(temp_df_att['WS_Attr_ID'])
 
        # concat list items into string
        temp_df_att['WS ALL Values'] = '; '.join(item for item in temp_df_att['val_counts'] if item)
#        temp_df_att['WS ALL Values'] = '; '.join(item for item in temp_df_att['WS_Attr_ID'] if item)
#        temp_df_att.to_csv('C:/Users/xcxg109/NonDriveFiles/hoist.csv')    
        
        #pull the top 10 values and put into 'Sample_Values' field
        all_vals = pd.concat([all_vals, temp_df_att], axis=0)

    if all_vals.empty == False:
        all_vals = all_vals[['WS_Attr_ID', 'WS ALL Values']]
        all_vals = all_vals.drop_duplicates(subset=['WS_Attr_ID'])

    return all_vals