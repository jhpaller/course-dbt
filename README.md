# Analytics engineering with dbt


All queries run in a session with
```
USE ROLE TRANSFORMER_DEV;
USE SCHEMA DEV_DB.DBT_JPALLERGMAILCOM;
USE WAREHOUSE TRANSFORMER_DEV_WH;
```

## Week 1 questions:

1. How many users do we have?
``` 
SELECT COUNT(*)
FROM STG_POSTGRES__USERS;
```
Answer: 130
2. On average, how many orders do we receive per hour?
``` 
WITH SUMMARY AS (SELECT DATEADD(HOUR,
                                DATE_PART(HOUR, CREATED_AT),
                                CREATED_AT::DATE) AS HOUR_BUCKET,
                        COUNT(*)                  AS ORDER_COUNT
                 FROM STG_POSTGRES__ORDERS
                 GROUP BY HOUR_BUCKET)
SELECT AVG(ORDER_COUNT)
FROM SUMMARY;
```
Answer: 7.5
3. On average, how long does an order take from being placed to being delivered?
``` 
SELECT AVG(DATEDIFF(DAY, CREATED_AT, DELIVERED_AT))
FROM STG_POSTGRES__ORDERS
WHERE STATUS = 'delivered';
```
Answer: 4 days (rounded)
4. How many users have only made one purchase? Two purchases? Three+ purchases?
``` 
WITH USER_METRICS AS (SELECT USER_ID,
                             COUNT(*) AS ORDER_COUNT
                      FROM STG_POSTGRES__ORDERS
                      GROUP BY USER_ID)
SELECT COUNT_IF(ORDER_COUNT = 1) AS USERS_WITH_1_ORDER,
       COUNT_IF(ORDER_COUNT = 2) AS USERS_WITH_2_ORDER,
       COUNT_IF(ORDER_COUNT > 2) AS USERS_WITH_3PLUS_ORDER
FROM USER_METRICS;
```
Answer: 25 (single), 28 (two), 71 (three or more)
5. On average, how many unique sessions do we have per hour?
``` 
WITH SUMMARY AS (SELECT DATEADD(HOUR,
                                DATE_PART(HOUR, CREATED_AT),
                                CREATED_AT::DATE)  AS HOUR_BUCKET,
                        COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSION_COUNT
                 FROM STG_POSTGRES__EVENTS
                 GROUP BY HOUR_BUCKET)
SELECT AVG(UNIQUE_SESSION_COUNT)
FROM SUMMARY;
```
Answer: 16.3 (rounded)