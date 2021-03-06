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
from queries_NUMERIC import gws_hier_query, gws_attr_query, \
        gamut_attr_query, grainger_attr_ETL_query
import query_code as q
import time

"""CODE TO SWITCH BETWEEN ORIGINAL FLAVOR GAMUT, TOOLBOX, AND GWS"""
#gws = GamutQuery()
gws = GWSQuery()
""" """
gcom = GraingerQuery()

STEP_ETL_query="""
            SELECT item.MATERIAL_NO AS Grainger_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS

            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            INNER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
        	--	AND item.DELETED_FLAG = 'N'
            
            --LEFT OUTER JOIN PRD_DWH_VIEW_LMT.material_v AS prod
            --    ON prod.MATERIAL = item.MATERIAL_NO
                
          --  LEFT OUTER JOIN PRD_DWH_VIEW_MTRL.supplier_v AS supplier
           --     ON prod.vendor = supplier.SUPPLIER_NO

            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '')
                AND item.SUPPLIER_NO NOT IN (20009997, 20201557, 20201186)
                AND {} IN ({})
            """

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
    grainger_df = gcom.grainger_q(grainger_basic_query, 'item.MATERIAL_NO', skus)

    return grainger_df


def stats(grainger_df):
    """ pull attributes by node and aggregate statistcs for the nodes"""
    gws_atts = pd.DataFrame()
    grainger_stats = pd.DataFrame()
    gws_att_count = pd.DataFrame()

    different_atts = pd.DataFrame()

    grainger_nodes = grainger_df['Category_ID'].unique()
        
    for node in grainger_nodes:
        remove_count = 0
        
        print('Grainger node = ', node)
        temp_df = grainger_df.loc[grainger_df['Category_ID']== node]
        
        grainger_skus = temp_df['#_Grainger_Products'].unique()
        gws_skus = temp_df['#_GWS_Products'].unique()

        gws_nodes = temp_df['GWS_Node_ID'].dropna().unique()

        temp_grainger_atts = gcom.grainger_q(grainger_attr_ETL_query, 'cat.CATEGORY_ID', node)

        if temp_grainger_atts.empty==False:
            temp_grainger_atts['count'] = 1

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
            
            revised_num = int(att_num - len(grainger_attributes))
                        
                        
            if gws_nodes.any():
                gws_atts = gws.gws_q(gws_attr_query, 'tax_att."categoryId"', gws_nodes[0])

                if gws_atts.empty==False:
                    gws_atts['#_GWS_Products'] = gws_skus[0]

                    gws_node_name = gws_atts['GWS_Node_Name'].unique()
                    gws_node_id = gws_atts['GWS_Node_ID'].unique()

                    gws_attributes = gws_atts['GWS_Attribute_Name'].to_list()

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
                
            temp_grainger_atts['#_Grainger_Attributes'] = grainger_att_count['count']
            temp_grainger_atts['#_Grainger_Products'] = grainger_skus[0]                                
            temp_grainger_atts = temp_grainger_atts.drop_duplicates(subset=['Grainger_Attr_ID'])

            grainger_stats = pd.concat([grainger_stats, temp_grainger_atts], axis=0)
            grainger_stats = grainger_stats.drop_duplicates(subset=['Category_ID'])
                            
    return grainger_stats
    

gws_df = pd.DataFrame()
grainger_df = pd.DataFrame()
grainger_stats = pd.DataFrame()
gws_stats_df = pd.DataFrame()
node_list = pd.DataFrame()

quer = 'CHECK'
search_level = 'tax.id'
ws = 'no'

data_type = fd.search_type()

if data_type == 'grainger_query':
    search_level = fd.blue_search_level()

search_data = fd.data_in(data_type, settings.directory_name)

start_time = time.time()
print('working...')


if data_type == 'grainger_query':
    if search_level == 'cat.CATEGORY_ID':
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
        for k in search_data:
            print('K = ', k)
            
            node_list = gcom.grainger_q(STEP_ETL_query, search_level, k)

            grainger_l3 = node_list['Category_ID'].unique()  #create list of pim nodes to pull
            print('grainger L3s = ', grainger_l3)

            for j in grainger_l3:
                print ('Grainger node = ', j)
                temp_df = gcom.grainger_q(STEP_ETL_query, 'cat.CATEGORY_ID', j)

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

print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
