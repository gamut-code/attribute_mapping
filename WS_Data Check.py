    # -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:00:31 2019

@author: xcxg109
"""

import file_data_GWS as fd
import settings_NUMERIC as settings
import pandas as pd

"""CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
#from gamut_query import GamutQuery
from GWS_query import GWSQuery
#from GWS_TOOLBOX_query import GWSQuery

""" """
from grainger_query import GraingerQuery
from queries_WS import gws_hier_query, gws_attr_query, STEP_ETL_query, \
        gamut_attr_query, grainger_attr_ETL_query, ETL_nodes_query
import WS_query_code as q
import time

"""CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT, TOOLBOX, AND GWS"""
#gws = GamutQuery()
gws = GWSQuery()
""" """
gcom = GraingerQuery()

pd.options.mode.chained_assignment = None

#variation of the basic query designed to include discontinued items
# EXCLUDE: (1) discontinued by Grainger (DG); (2) discontinued by Vendor (DV); (3) Customer Specific Inventory (CS)
# EXCLUDE: RMC = L15 (Zoro only products), RMC = blank (Canada only, Mexico only products)
# EXCLUDE: Supplier No = 20009997, 20201557, 20201186 (7-Combo products)

            
def gws_data(df):
    sku_list = df['Grainger_SKU'].tolist()

    gws_skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    
    """CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT AND GWS"""
#    gws_df = gamut.gamut_q(gamut_hier_query, 'tprod."supplierSku"', gws_skus)
    gws_df = gws.gws_q(gws_hier_query, 'tprod."gtPartNumber"', gws_skus)
    """ """

    return gws_df


def grainger_data(gws_df):
    sku_list = gws_df['Grainger_SKU'].tolist()
    skus = ", ".join("'" + str(i) + "'" for i in sku_list)
    grainger_df = gcom.grainger_q(STEP_ETL_query, 'item.MATERIAL_NO', skus)

    return grainger_df


def stats(grainger_df):
    """ pull attributes by node and aggregate statistcs for the nodes"""
    gws_atts = pd.DataFrame()
    grainger_stats = pd.DataFrame()
    gws_att_count = pd.DataFrame()

    grainger_nodes = grainger_df['Category_ID'].unique()

    for node in grainger_nodes:
        temp_df = grainger_df.loc[grainger_df['Category_ID']== node]
        cat_name = temp_df['Category_Name'].unique()
        print('grainger node = {} {}'.format(cat_name, node))

        grainger_skus = temp_df['#_Grainger_Products'].unique()
        gws_skus = temp_df['#_GWS_Products'].unique()

        gws_nodes = temp_df['GWS_Node_ID'].dropna().unique()
        if gws_nodes.size != 0:
            gws_nodes = gws_nodes[0]
        else:
            gws_nodes = ''
            
        gws_name = temp_df['GWS_Node_Name'].dropna().unique()
        if gws_name.size != 0:
            gws_name = gws_name[0]
        else:
            gws_name = ''

        temp_df['#_Grainger_Products'] = grainger_skus[0]                                
        temp_df['#_GWS_Products'] = gws_skus[0]

        temp_grainger_atts = allCATS_df.loc[allCATS_df['Category_ID']== node]
        temp_grainger_atts = temp_grainger_atts.reset_index()             

        if temp_grainger_atts.empty==False:
            temp_grainger_atts['count'] = 1

            temp_grainger_atts['GWS_Node_Name'] = gws_name
            temp_grainger_atts['GWS_Node_ID'] = gws_nodes
            temp_grainger_atts['#_GWS_Products'] = 0
            temp_grainger_atts['#_GWS_Attributes'] = 0
            temp_grainger_atts['Differing_Attributes'] = ''
            temp_grainger_atts['Grainger_Attributes'] = ''
            temp_grainger_atts['GWS_Attributes'] = ''

#            temp_grainger_atts['Grainger_Attribute_Name'] = temp_grainger_atts['Grainger_Attribute_Name'].str.strip()

            # remove white spaces before and after values
            grainger_attributes = temp_grainger_atts['Grainger_Attribute_Name'].unique().tolist()
            att_num = len(grainger_attributes)
            
            # remove Item and Series from attribute counts (** specific terms)
            i = 'Item' in grainger_attributes
            s = 'Series' in grainger_attributes   
                        
            if i: 
                grainger_attributes.remove('Item')
            if s: 
                grainger_attributes.remove('Series')

            # remove 'Green' attributes based on general pattern match
            grainger_attributes = [ x for x in grainger_attributes if 'Green Certification' not in x ]
            grainger_attributes = [ x for x in grainger_attributes if 'Green Environmental' not in x ]
            
            grainger_attributes = map(str.strip, grainger_attributes)
            grainger_attributes = list(filter(None, grainger_attributes)) 

            revised_num = int(att_num - len(grainger_attributes))

            if grainger_attributes:
                grainger_attributes = sorted(grainger_attributes)
                temp_grainger_atts['Grainger_Attributes'] = "; ".join(item for item in grainger_attributes if item)

            if gws_nodes != '':
                gws_atts = gws.gws_q(gws_attr_query, 'tax_att."categoryId"', gws_nodes)

                if gws_atts.empty==False:
                    gws_atts['#_GWS_Products'] = gws_skus[0]

                    gws_node_name = gws_atts['GWS_Node_Name'].unique()
                    gws_node_id = gws_atts['GWS_Node_ID'].unique()

                    gws_attributes = gws_atts['GWS_Attribute_Name'].to_list()

                    if gws_attributes:
                        gws_attributes = sorted(gws_attributes)
                        temp_grainger_atts['GWS_Attributes'] = "; ".join(item for item in gws_attributes if item)

                    set_difference = set(grainger_attributes) - set(gws_attributes)
                    diff = list(set_difference)
                    diff = '; '.join(diff)

                    gws_atts['count'] = 1 
                    gws_att_count = gws_atts.drop_duplicates(subset=['GWS_Attr_ID'])        
                    gws_att_count = gws_att_count.groupby(['GWS_Node_Name', 'GWS_Node_ID', '#_GWS_Products'])['count'].sum().reset_index()

                    grainger_att_count = temp_grainger_atts.drop_duplicates(subset=['Grainger_Attr_ID'])
                    grainger_att_count = grainger_att_count.groupby(['Category_ID'])['count'].sum().reset_index()
                    grainger_att_count = grainger_att_count - revised_num  # take into account if item or series were removed

                    temp_grainger_atts = temp_grainger_atts.merge(gws_att_count, how="left", left_on=["Category_Name"], \
                                                                                  right_on=['GWS_Node_Name'])
                    temp_grainger_atts['#_GWS_Attributes'] = gws_att_count['count']
                    temp_grainger_atts['#_GWS_Products'] = gws_skus[0]
                    temp_grainger_atts['GWS_Node_Name'] = gws_node_name[0]
                    temp_grainger_atts['GWS_Node_ID'] = gws_node_id[0]
                    temp_grainger_atts['Differing_Attributes'] = diff

                else:
                    grainger_att_count = temp_grainger_atts.drop_duplicates(subset=['Grainger_Attr_ID'])
                    grainger_att_count = grainger_att_count.groupby(['Category_ID'])['count'].sum().reset_index()
                    grainger_att_count = grainger_att_count - revised_num # take into account if item or series were removed

            else:
                print('No GWS Nodes')
           
                grainger_att_count = temp_grainger_atts.drop_duplicates(subset=['Grainger_Attr_ID'])
                grainger_att_count = grainger_att_count.groupby(['Category_ID'])['count'].sum().reset_index()
                
            temp_grainger_atts['#_GWS_Products'] = gws_skus[0]
            temp_grainger_atts['#_Grainger_Attributes'] = grainger_att_count['count']
            temp_grainger_atts['#_Grainger_Products'] = grainger_skus[0]                                
            temp_grainger_atts = temp_grainger_atts.drop_duplicates(subset=['Grainger_Attr_ID'])

            temp_grainger_atts = temp_grainger_atts[['Segment_ID','Segment_Name','Family_ID','Family_Name','Category_ID',\
                            'Category_Name','GWS_Node_Name','GWS_Node_ID','#_GWS_Products','#_GWS_Attributes',\
                            'Differing_Attributes','#_Grainger_Attributes','#_Grainger_Products','Grainger_Attributes',\
                            'GWS_Attributes']]

#            filename = 'C:/Users/xcxg109/NonDriveFiles/temp_atts_'+str(node)+'.csv'
#            temp_grainger_atts.to_csv(filename)
            
#            grainger_stats = pd.concat([grainger_stats, temp_grainger_atts], axis=0)            
#            grainger_stats = grainger_stats.drop_duplicates(subset=['Category_ID']) 
        
        else:
            print('GRAINGER NODE {} NO ATTRIBUTES'.format(node))
            print('GWS NODES = ', gws_nodes)
    
            if gws_nodes != '':
                temp_grainger_atts = temp_df
                gws_atts = gws.gws_q(gws_hier_query, 'tprod."categoryId"', gws_nodes)
                    
                if gws_atts.empty==False:
                    gws_atts['#_GWS_Products'] = gws_skus[0]

                    gws_node_name = gws_atts['GWS_Node_Name'].unique()
                    gws_node_id = gws_atts['GWS_Node_ID'].unique()

                    temp_grainger_atts['#_GWS_Attributes'] = 0 
                    temp_grainger_atts['#_GWS_Products'] = gws_skus[0]
                    temp_grainger_atts['#_Grainger_Attributes'] = 0
                    temp_grainger_atts['GWS_Node_Name'] = gws_node_name[0]
                    temp_grainger_atts['GWS_Node_ID'] = gws_node_id[0]
                    temp_grainger_atts['Differing_Attributes'] = ''

                else:

                    temp_grainger_atts['GWS_Node_Name'] = temp_df['GWS_Category_Name'].unique()
                    temp_grainger_atts['GWS_Node_ID'] = temp_df['GWS_Category_ID'].unique()
                    temp_grainger_atts['#_GWS_Attributes'] = 0
                    temp_grainger_atts['#_GWS_Products'] = 0
                    temp_grainger_atts['Differing_Attributes'] = ''

#                    filename = 'C:/Users/xcxg109/NonDriveFiles/temp_atts_'+str(gws_node_id[0])+'.csv'
#                    temp_grainger_atts.to_csv(filename)
                
            else:
                temp_grainger_atts = temp_df

                temp_grainger_atts['#_GWS_Attributes'] = ''
                temp_grainger_atts['#_GWS_Products'] = 0
                temp_grainger_atts['GWS_Node_Name'] = ''
                temp_grainger_atts['GWS_Node_ID'] = ''
                temp_grainger_atts['Differing_Attributes'] = ''
        
        grainger_stats = pd.concat([grainger_stats, temp_grainger_atts], axis=0)            
        grainger_stats = grainger_stats.drop_duplicates(subset=['Category_ID']) 

  #  temp_grainger_atts.to_csv('C:/Users/xcxg109/NonDriveFiles/temp_no_ATTS.csv')    
    
    
    return grainger_stats
    

gws_df = pd.DataFrame()
grainger_df = pd.DataFrame()
grainger_stats = pd.DataFrame()
gws_stats_df = pd.DataFrame()
node_list = pd.DataFrame()
allCATS = pd.DataFrame()

quer = 'CHECK'
search_level = 'tax.id'
ws = 'no'

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.WS_search_type()


start_time = time.time()
print('working...')


if data_type == 'grainger_query':
    if search_level == 'cat.CATEGORY_ID':
        search_data = fd.data_in(data_type, settings.directory_name)

        for k in search_data:
            print('k = ', k)

            temp_df = gcom.grainger_q(STEP_ETL_query, search_level, k)

            if temp_df.empty == False:
                grainger_skus = len(temp_df['Grainger_SKU'])
                temp_df['#_Grainger_Products'] = grainger_skus

                gws_df = gws_data(temp_df)

                if gws_df.empty == False:
                    ws = 'yes'
                    gws_skus = len(gws_df['GWS_SKU'])

                    #if GWS data is present for these skus, merge with grainger data
                    temp_df = temp_df.merge(gws_df, how="left", on=["Grainger_SKU"])
                    temp_df['#_GWS_Products'] = gws_skus
                else:
                    temp_df['#_GWS_Products'] = ''
                    temp_df['GWS_Node_ID'] = ''

                grainger_df = pd.concat([grainger_df, temp_df], axis=0)

            else:
                print('{} All SKUs are Excluded'.format(k))

        if grainger_df.empty==False:    
            grainger_stats = stats(grainger_df)
            fd.data_check_out(settings.directory_name, grainger_df, grainger_stats, quer, search_level, ws)

    else:
        allCATS_df = q.get_att_values()

        seg = allCATS_df['Segment_ID'].unique()
        seg = seg[0]

        print('segment ID = ', seg)

        grainger_all_nodes = gcom.grainger_q(ETL_nodes_query, 'cat.SEGMENT_ID', seg)
        grainger_all_nodes['Count'] = 1
        
        grainger_l3 = pd.DataFrame(grainger_all_nodes.groupby(['Category_ID'])['Count'].sum()).reset_index()
        grainger_l3 = grainger_l3['Category_ID']

        nodes = len(grainger_l3)
        node_count = 1

        for l3 in grainger_l3:            
            print ('node count = {} of {} : L3= {}'.format(node_count, nodes, l3))

            temp_df = gcom.grainger_q(STEP_ETL_query, 'cat.CATEGORY_ID', l3)
       
            if temp_df.empty == False:
                grainger_skus = len(temp_df['Grainger_SKU'])
                temp_df['#_Grainger_Products'] = grainger_skus
                
                gws_df = gws_data(temp_df)

                if gws_df.empty == False:
                    ws = 'yes'
                    gws_skus = len(gws_df['GWS_SKU'])

                    #if GWS data is present for these skus, merge with grainger data
                    temp_df = temp_df.merge(gws_df, how="left", on=["Grainger_SKU"])
                    temp_df['#_GWS_Products'] = gws_skus
                else:
                    temp_df['#_GWS_Products'] = ''
                    temp_df['GWS_Node_ID'] = ''

                grainger_df = pd.concat([grainger_df, temp_df], axis=0)

            else:
                print('{} All SKUs are Excluded'.format(l3))

            node_count = node_count + 1
            
    if grainger_df.empty==False:    
        grainger_stats = stats(grainger_df)
        fd.data_check_out(settings.directory_name, grainger_df, grainger_stats, quer, search_level, ws)

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
