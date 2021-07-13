"""
Search for values using LIKE, edit query for specific values
"""

import pandas as pd
import numpy as np
import Attribute_Value_Counts_adjunct_query as adj
from GWS_query import GWSQuery
import time

gws = GWSQuery()
pd.options.mode.chained_assignment = None


#def test_query(search, k):
attr_values="""
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
					array_to_string(tax.ancestor_names || tax.name,' > ') as "PIM_Path"
				, tax.ancestors[1] as "WS_Branch_ID"
				, tax.ancestor_names[1] as "WS_Branch_Name"
				, tprod."categoryId" AS "WS_Leaf_Node_ID"
				, tax.name as "WS_Leaf_Node_Name"
				, tprod."gtPartNumber" as "WS_SKU"
				, tax_att.id as "WS_Attr_ID"
				, tax_att."dataType" as "Data_Type"
				, tax_att.name as "WS_Attribute_Name"
				, tprodvalue."valueNormalized" as "Normalized_Value"
				, tprodvalue."unitNormalized" as "Normalized_Unit"

		FROM  taxonomy_product tprod

		INNER JOIN tax
				ON tax.id = tprod."categoryId"
				--  AND (4458 = ANY(tax.ancestors)) --OR 8215 = ANY(tax.ancestors) OR 7739 = ANY(tax.ancestors))  -- *** ADD TOP LEVEL NODES HERE ***
				AND tprod.status = 3

		INNER JOIN taxonomy_attribute tax_att
				ON tax_att."categoryId" = tprod."categoryId"
				AND tax_att.deleted = 'false'

		INNER JOIN  taxonomy_product_attribute_value tprodvalue
				ON tprod.id = tprodvalue."productId"
				AND tax_att.id = tprodvalue."attributeId"
				AND tprodvalue.deleted = 'false'

		WHERE {} LIKE {}
            AND tax_att."dataType" = 'text'
"""


def get_col_widths(df):
    #find maximum length of the index column
    idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
    #Then concatenate this to max of the lengths of column name and its values for each column
    
    return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in df.columns]


def data_out(final_df, batch=''):
    outfile = 'C:/Users/xcxg109/NonDriveFiles/ATTR_VALUES_'+str(batch)+'.xlsx'
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter', options={'strings_to_urls': False})
 
    final_df.to_excel (writer, sheet_name="Attributes", startrow=0, startcol=0, index=False)
    worksheet1 = writer.sheets['Attributes']
    workbook  = writer.book

    col_widths = get_col_widths(final_df)
    col_widths = col_widths[1:]
    
    for i, width in enumerate(col_widths):
        if width > 40:
            width = 40
        elif width < 10:
            width = 10
        worksheet1.set_column(i, i, width)

    layout = workbook.add_format()
    layout.set_text_wrap('text_wrap')
    layout.set_align('left')

    writer.save()


ws_df = pd.DataFrame()

start_time = time.time()
print('working...')

# read in attribute name from file -- fix this to choice menu at some point
att_df = pd.read_csv('C:/Users/xcxg109/NonDriveFiles/reference/MAGGIES_problem_atts.csv')

attributes = att_df['Attribute'].unique().tolist()
print('attribute # = ', len(attributes))

count_att = 1

for att in attributes:
    print('{}. {}'.format(count_att, att))

    att = "'" + att + "'"
    temp_df = gws.gws_q(attr_values, 'tax_att.name', att)

#    if temp_df.empty == False:
#        for row in temp_df.itertuples():
#            val = str(temp_df.at[row.Index, 'Normalized_Value'])
#            unit = str(temp_df.at[row.Index, 'Normalized_Unit'])
            
#            if unit == '':
#                temp_df.at[row.Index, 'Final_Value'] = val

#            else:
#                temp_df.at[row.Index, 'Final_Value'] = val + ' ' + unit
        
    ws_df = pd.concat([ws_df, temp_df], axis=0, sort=False)
    
#    else:
#        print('EMPTY DATAFRAME')

    count_att += 1
    
counts_df = adj.attr_values(ws_df)

ws_df = ws_df.merge(counts_df, how="left", on=['WS_Attr_ID'])

ws_df = ws_df.drop(columns=['WS_SKU', 'Normalized_Value', 'Normalized_Unit'])
ws_df = ws_df.drop_duplicates()

if len(ws_df) > 900000:
		count = 1
		# split into multiple dfs of 40K rows, creating at least 2
		num_lists = round(len(ws_df)/900000, 0)
		num_lists = int(num_lists)
		if num_lists == 1:
				num_lists = 2
		print('creating {} output files'.format(num_lists))
		# np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
		split_df = np.array_split(ws_df, num_lists)
		for object in split_df:
				print('iteration {} of {}'.format(count, num_lists))
				data_out(object, count)
				count += 1
else:
		data_out(ws_df)


print("--- {} minutes ---".format(round((time.time() - start_time)/60, 2)))