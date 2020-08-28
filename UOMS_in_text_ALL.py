# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 13:32:56 2020

@author: xcxg109
"""

import pandas as pd
import numpy as np
import re
import string
from collections import defaultdict
from GWS_query import GWSQuery
import file_data_GWS as fd
import time
import settings_NUMERIC as settings


gws = GWSQuery()


gws_values_single="""
        WITH RECURSIVE tax AS (
                SELECT  id,
            name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
                FROM    taxonomy_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false

                UNION ALL

                SELECT  category.id,
            category.name,
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "Gamut_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , tprod."categoryId" AS "WS_Node_ID"
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        --  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."dataType" = 'text'
        AND {} IN ({})
        """

gws_values_group="""
        WITH RECURSIVE tax AS (
                SELECT  id,
            name,
            ARRAY[]::INTEGER[] AS ancestors,
            ARRAY[]::character varying[] AS ancestor_names
                FROM    taxonomy_category as category
                WHERE   "parentId" IS NULL
                AND category.deleted = false

                UNION ALL

                SELECT  category.id,
            category.name,
            tax.ancestors || tax.id,
            tax.ancestor_names || tax.name
                FROM    taxonomy_category as category
                INNER JOIN tax ON category."parentId" = tax.id
                WHERE   category.deleted = false

            )

    SELECT
          array_to_string(tax.ancestor_names || tax.name,' > ') as "WS_PIM_Path"
        , tax.ancestors[1] as "WS_Category_ID"  
        , tax.ancestor_names[1] as "WS_Category_Name"
        , {} AS "WS_Node_ID"                    -- CHEAT INSERT OF 'tprod."categoryId"' HERE SO THAT I HAVE THE 3 ELEMENTS FOR A QUERY
        , tax.name as "WS_Node_Name"
        , tprod."gtPartNumber" as "WS_SKU"
        , tax_att.id as "WS_Attr_ID"
        , tax_att.name as "WS_Attribute_Name"
        , tprodvalue.value as "Original_Value"
        , tprodvalue."valueNormalized" as "Normalized_Value"
   
    FROM  taxonomy_product tprod

    INNER JOIN tax
        ON tax.id = tprod."categoryId"
        AND ({} = ANY(tax.ancestors)) -- *** TOP LEVEL NODE GETS ADDED HERE ***

    INNER JOIN taxonomy_attribute tax_att
        ON tax_att."categoryId" = tprod."categoryId"

    INNER JOIN  taxonomy_product_attribute_value tprodvalue
        ON tprod.id = tprodvalue."productId"
        AND tax_att.id = tprodvalue."attributeId"
        
    WHERE tax_att."dataType" = 'text'
        """


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]

    
def determine_uoms(df, uom_df, current_uoms):
    """for all non 'text' data types, compare 'String' field to our UOM list, then compare these potential UOMs
    to current GWS UOM groupings. finally, determine whether numeric part of the value is a fraction or decimal"""

    text_list = list()
    text_list_lower = list()
    uom_list = list()
    cur_uoms = list()

    # build unique UOM list for comparison -- all uoms + current acceptable ones
    uom_list = uom_df['unit_name'].tolist()
    uom_list = set(uom_list)

    cur_uoms = current_uoms['Published Description'].tolist()
    cur_uoms = set(cur_uoms)
    
    # search through rows of the df (NOTE: df passed here is only for a single attribute) and look at individual values
    for row in df.itertuples():
        potential_list = list()
        
        str_value = df.at[row.Index,'Normalized_Value']
        str_value = str(str_value)

        str_value_lower = str_value.lower()
            
        # force 'Normalized_Value' content into a list so we can evaulate the entire string for a match against uom_list
        text_list = str_value.split(' ')
        text_list_lower = str_value_lower.split(' ')
        
        # if 'Normalized_Value' field contains value(s), compare to UOM list and assigned to 'Potential UOMs'
        if str_value != '':
            # check for a match of the entire contant of 'Normalized_Value' against our uom_list
            match = set(text_list).intersection(set(uom_list))
            # but if we don't find an exact match, parse 'Normalized_Value' content and attempt to match up with uom_List
            
            if match:
                potential_list.append(match)

            else:
                match = set(text_list_lower).intersection(set(uom_list))

                if match:
                    potential_list.append(match)

                else:
                    pot_uom = [x for x in uom_list if x in str_value.split()]
                    # if parse by word match still fails, try one more time at a more granular level for a match                    

                    if pot_uom:
                        potential_list.append(pot_uom)

                    else:
                        pot_uom = [x for x in uom_list if x in str_value_lower.split()]

                        if pot_uom:
                            potential_list.append(pot_uom)

            last_chance = [x for x in uom_list if x in str_value]

            if '"' in last_chance:                        
                last_chance = '"'
                potential_list.append(last_chance)
            else:
                last_chance = ''
                
            if potential_list:
                df.at[row.Index, 'Potential_UOMs'] = potential_list
                """NOTE: This piece was written as a potential suggest/replace based on UOMs found in the list -- not completed"""                
#                update = set(text_list).intersection(set(cur_uoms))
            
#                if update:
#                    pot_value.append(update)

#                else:
#                    update = set(text_list_lower).intersection(set(cur_uoms))

#                    if update:
#                        pot_value.append(update)

#                    else:
#                        sec_chance = [x for x in cur_uoms if x in str_value.split()]
#                        # if parse by word match still fails, try one more time at a more granular level for a match                    

#                        if sec_chance:
#                            pot_value.append(sec_chance)

#                        else:
#                            sec_chance = [x for x in cur_uoms if x in str_value_lower.split()]

#                            if sec_chance:
#                                pot_value.append(sec_chance)
                                
    return df


# get uom list
filename = 'C:/Users/xcxg109/NonDriveFiles/reference/UOM_data_sheet.csv'
uom_df = pd.read_csv(filename)
# create df of the lovs and their concat values

# get accepted WS uom list
filename = 'C:/Users/xcxg109/NonDriveFiles/reference/WS_units.csv'
current_uoms = pd.read_csv(filename)
# create df of the lovs and their concat values


ws_df = pd.DataFrame()
data_type = 'gws_query'

while True:
    try:
        search_level = input("Search by: \n1. Node Group \n2. Single Category ")
        if search_level in ['1', 'g', 'G']:
            search_level = 'group'
            break
        elif search_level in ['2', 's', 'S']:
            search_level = 'single'
            break
    except ValueError:
        print('Invalid search type')
        
search_data = fd.data_in(data_type, settings.directory_name)

print('working...')

for node in search_data:
    start_time = time.time()

    if search_level == 'single':
        df = gws.gws_q(gws_values_single, 'tprod."categoryId"', node)
    elif search_level == 'group':
        df = gws.gws_q(gws_values_group, 'tprod."categoryId"', node)
        
    print('k = ', node)
    
    if df.empty == False:
        atts = df['WS_Attr_ID'].unique()

        df['Potential_UOMs'] = ''
        df['Recommended_Value_Update'] = ''

        for attribute in atts:
            temp_df = df.loc[df['WS_Attr_ID']== attribute]
            temp_df = determine_uoms(temp_df, uom_df, current_uoms)

            ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False) #add prepped df for this gws node to the final df

        ws_df = ws_df[ws_df.Potential_UOMs != '']
        ws_df = ws_df.sort_values(['WS_Category_Name', 'WS_Node_Name', 'WS_Attribute_Name'], ascending=[True, True, True])

        outfile = 'C:/Users/xcxg109/NonDriveFiles/reference/'+str(node)+'_text_UOMs.xlsx'  
        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
        workbook  = writer.book

        ws_df.to_excel (writer, sheet_name="LOVs", startrow=0, startcol=0, index=False)

        worksheet = writer.sheets['LOVs']

        layout = workbook.add_format()
        layout.set_text_wrap('text_wrap')
        layout.set_align('left')

        col_widths = get_col_widths(df)
        col_widths = col_widths[1:]
    
        for i, width in enumerate(col_widths):
            if width > 40:
                width = 40
            elif width < 10:
                width = 10
            worksheet.set_column(i, i, width)

        worksheet.set_column('J:J', 50, layout)

        writer.save()

    else:
        print('{} No attribute data'.format(node))

    print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))
