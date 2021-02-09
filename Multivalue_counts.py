 # -*- coding: utf-8 -*-

import time


"""
Spyder Editor

This is a temporary script file.
"""


#GWS New Workstation Test
from postgres_GWS import PostgresDatabase_GWS
from pathlib import Path
import settings_NUMERIC as settings
from GWS_query import GWSQuery

gws = GWSQuery()
moist = PostgresDatabase_GWS()


# no need for an open connection,
# as we're only doing a single query
#engine.dispose()


#def test_query(search, k):
multi_atts="""
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
 tprod."gtPartNumber" as "WS_SKU"
 , {} AS "WS_Node_ID"               --  cheat to use tprod."categoryId" to make the group search work
, tax_att.id as "WS_Attr_ID"
, tprodvalue.id as "WS_Attr_Value_ID"

FROM taxonomy_product tprod

INNER JOIN tax
    ON tax.id = tprod."categoryId"
    AND ({} = ANY(tax.ancestors)) -- *** ADD TOP LEVEL NODES HERE ***
    AND tprod.status = 3

INNER JOIN taxonomy_attribute tax_att
    ON tax_att."categoryId" = tprod."categoryId"
    AND tax_att.deleted = 'false'

INNER JOIN  taxonomy_product_attribute_value tprodvalue
    ON tprod.id = tprodvalue."productId"
    AND tax_att.id = tprodvalue."attributeId"
    AND tprodvalue.deleted = 'false'
    AND tax_att."multiValue" = 'true'

INNER JOIN taxonomy_category cat
    ON cat.id = tprod."categoryId"
    
"""

import pandas as pd
import file_data_GWS as fd


#request the type of data to pull: blue or yellow, SKUs or node, single entry or read from file
data_type = fd.search_type()

#ask user for node number/SKU or pull from file if desired    
search_data = fd.data_in(data_type, settings.directory_name)


start_time = time.time()
print('working...')

total_df = pd.DataFrame()


for node in search_data:
    temp_df = gws.gws_q(multi_atts, 'tprod."categoryId"', node)
    temp_df['Count'] = 1

    temp_df = pd.DataFrame(temp_df.groupby(['WS_SKU', 'WS_Attr_ID'])['Count'].sum())
    temp_df = temp_df.reset_index()
    
    total_df = pd.concat([total_df, temp_df], axis=0, sort=False)
        
total_df = total_df[total_df['Count'] > 1]
total_df = total_df.drop_duplicates(subset=['WS_SKU', 'WS_Attr_ID'])

if len(temp_df) > 1000000:
    count = 1
    num_lists = round(len(temp_df)/45000, 0)
    num_lists = int(num_lists)

    if num_lists == 1:
        num_lists = 2
    
    print('creating {} output files'.format(num_lists))

    # np.array_split creates [num_lists] number of chunks, each referred to as an object in a loop
    split_df = np.array_split(temp_df, num_lists)

    for object in split_df:
        print('iteration {} of {}'.format(count, num_lists))

        outfile = Path(settings.directory_name)/"multis_"+count+".csv"
        total_df.to_csv(outfile)

        count += 1
    
# if original df < 30K rows, process the entire thing at once
else:
  outfile = Path(settings.directory_name)/"multis.csv"
  total_df.to_csv(outfile)

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))
