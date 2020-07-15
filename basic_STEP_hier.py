 # -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from graingerio import TeraClient
import time


tc = TeraClient()

#def test_query(search, k):
test_q="""
            SELECT cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name

            FROM PRD_DWH_VIEW_MTRL.CATEGORY_V cat
                                            
            WHERE cat.SEGMENT_ID = (23717)
            """


start_time = time.time()
print('working...')

grainger_df = tc.query(test_q)

grainger_df.to_csv ('C:/Users/xcxg109/NonDriveFiles/basic_HIER.csv')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

