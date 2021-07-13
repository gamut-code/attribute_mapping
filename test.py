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
 SELECT item.MATERIAL_NO AS STEP_SKU
            , cat.SEGMENT_ID AS Segment_ID
            , cat.SEGMENT_NAME AS Segment_Name
            , cat.FAMILY_ID AS Family_ID
            , cat.FAMILY_NAME AS Family_Name
            , cat.CATEGORY_ID AS Category_ID
            , cat.CATEGORY_NAME AS Category_Name
            , item.SUPPLIER_NO
            , item.RELATIONSHIP_MANAGER_CODE
            , item.PM_CODE
            , item.SALES_STATUS
            , item.PRICING_FLAG
            , item.PRICER_FIRST_EFFECTIVE_DATE
            
            FROM PRD_DWH_VIEW_LMT.ITEM_V AS item

            FULL OUTER JOIN PRD_DWH_VIEW_MTRL.CATEGORY_V AS cat
            	ON cat.CATEGORY_ID = item.CATEGORY_ID
--         		AND item.DELETED_FLAG = 'N'
--                AND item.PRODUCT_APPROVED_US_FLAG = 'Y'
--                AND item.PM_CODE NOT IN ('R9')
        
            WHERE item.SALES_STATUS NOT IN ('DG', 'DV', 'CS')
                AND item.RELATIONSHIP_MANAGER_CODE NOT IN ('L15', '') -- NOTE: blank RMC = MX only
                AND item.MATERIAL_NO IN ('12R249 ', '45HE83 ', '162D73 ', '58YE09 ', '4UAP3 ', '5LUJ4 ', '20FW96 ', '1YNN8 ', '31NJ82 ', '55EY25 ', '55EY29 ', '55EY22 ', '2VHP8 ', '60GV86 ', '43WM30 ', '43WM20 ', '43WM40 ', '43WM32 ', '43WM39 ', '65RK12 ', '30TJ95 ', '1UNH7 ', '4PKU3 ', '1AKX8 ', '3XU40 ', '303W15 ', '10J863 ', '41XA08 ', '527D17 ', '329FV3 ', '6ACH3 ', '20XM25 ', '2HRC1 ', '482L15 ', '15F068 ', '24KP94 ', '24KP98 ', '49H741 ', '49H744 ', '24KP82 ', '24KP86 ', '24KP99 ', '24KP85 ', '24KR10 ', '24KP96 ', '24KR15 ', '24KP88 ', '24KP89 ', '24KP92 ', '24KP84 ', '24KR03 ', '24KR06 ', '24KR02 ', '24KP80 ', '24KR16 ', '24KP79 ', '49H746 ', '24KP91 ', '24KR11 ', '24KR09 ', '24KP83 ', '24KP97 ', '24KR07 ', '24KR13 ', '24KR08 ', '24KR12 ', '24KR05 ', '49H743 ', '24KR04 ', '49H745 ', '24KP93 ', '24KR17 ', '24KP90 ', '24KR14 ', '24KP81 ', '24KP87 ', '24KR01 ', '49H742 ', '24KP95 ', '1UKR1 ', '1UY96 ', '4GPW8 ', '3RTT7 ', '56JD59 ', '29UP62 ', '236FY6 ', '29UP63')
"""

start_time = time.time()
print('working...')

grainger_df = tc.query(test_q)

grainger_df.to_csv ('C:/Users/xcxg109/NonDriveFiles/TEST OUTPUT.csv')

print("--- {} seconds ---".format(round(time.time() - start_time, 2)))

