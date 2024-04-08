# Analytics engineering with dbt


All queries run in a session with
``` sql
USE ROLE TRANSFORMER_DEV;
USE SCHEMA DEV_DB.DBT_JPALLERGMAILCOM;
USE WAREHOUSE TRANSFORMER_DEV_WH;
```

## Week 3 questions:

### Part 1
#### What is our overall conversion rate?

``` sql
-- defensive version, slightly unnecessary given FACT_PAGE_VIEWS construction
WITH PURCHASES AS (SELECT COUNT(DISTINCT SESSION_ID) AS SESSION_COUNT
                   FROM FACT_PAGE_VIEWS
                   WHERE CHECKOUT_COUNT > 0),
     VIEWS AS (SELECT COUNT(DISTINCT SESSION_ID)     AS SESSION_COUNT
               FROM FACT_PAGE_VIEWS
               WHERE PAGE_VIEW_COUNT > 0)
SELECT PURCHASES.SESSION_COUNT / VIEWS.SESSION_COUNT AS CONVERSION_RATE
FROM PURCHASES,
     VIEWS;

-- simple version using existing user order aggregation
SELECT SUM(ORDER_COUNT) / SUM(SESSION_COUNT) AS CONVERSION_RATE
FROM FACT_USER_ORDER;
```
Answer: 62.5% (0.624567 rounded)

#### What is our conversion rate by product?

``` sql
-- defensive version, but unnecessary given FACT_PAGE_VIEWS construction
WITH PRODUCT_SESSIONS AS (SELECT DP.NAME                      AS PRODUCT_NAME,
                                 SESSION_ID,
                                 COUNT(*)                     AS VIEW_COUNT,
                                 COUNT_IF(CHECKOUT_COUNT > 0) AS PURCHASE_COUNT
                          FROM FACT_PAGE_VIEWS AS FPV
                                   JOIN DIM_PRODUCTS AS DP USING (PRODUCT_ID)
                          WHERE PAGE_VIEW_COUNT > 0
                          GROUP BY 1, 2)
SELECT PRODUCT_NAME,
       SUM(PURCHASE_COUNT) / SUM(VIEW_COUNT) AS CONVERSION_RATE
FROM PRODUCT_SESSIONS
GROUP BY 1
ORDER BY 1;

-- clean version
SELECT DP.NAME                                 AS PRODUCT_NAME,
       COUNT_IF(CHECKOUT_COUNT > 0) / COUNT(*) AS CONVERSION_RATE
FROM FACT_PAGE_VIEWS AS FPV
         JOIN DIM_PRODUCTS AS DP USING (PRODUCT_ID)
GROUP BY 1
ORDER BY 1;
```
Answer: varies from 34.4% (Pothos) to 60.1% (String of pearls)

### Part 2
#### Create a macro to simplify part of a model(s)
* row_number 
  * simplify the generation of row_number to persist the ability to order ascending or descending (est first or last)
  * Used in fact_sessions and fact_orders 
* sum_of
  * clean and simplify the aggregated pivot of page visits, easily extensible
  * used in fact_page_views

### Part 3
#### Add a post hook to your project to apply grants to the role “reporting”.
```sql
-- validation
SHOW GRANTS TO ROLE REPORTING;
SELECT $2 AS PRIVILEGE, 
       $3 AS GRANTED_ON, 
       $4 AS NAME
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
WHERE $6 = 'REPORTING'
  AND $4 LIKE 'DEV_DB.DBT_JPALLERGMAILCOM.%';

--+---------+----------+----------------------------------------------------+
--|PRIVILEGE|GRANTED_ON|NAME                                                |
--+---------+----------+----------------------------------------------------+
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.DIM_PRODUCTS             |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.DIM_PROMOS               |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.DIM_USERS                |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.FACT_ORDERS              |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.FACT_PAGE_VIEWS          |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.FACT_SESSIONS            |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.FACT_USER_ORDER          |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.INT_ORDER_PRODUCTS       |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.INT_SESSIONS             |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__ADDRESSES  |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__EVENTS     |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__ORDERS     |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__ORDER_ITEMS|
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__PRODUCTS   |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__PROMOS     |
--|SELECT   |VIEW      |DEV_DB.DBT_JPALLERGMAILCOM.STG_POSTGRES__USERS      |
--+---------+----------+----------------------------------------------------+
```
### Part 4
#### Install a package  and apply one or more of the macros to your project

 * dbt-utils 
   * dbt_utils.star
     * dim_promos
     * dim_products
     * dim_user (and simplified PII removal)
   * dbt_utils.group_by
     * fact_page_views

### Part 5
#### see uploaded image

### Part 6
#### Which products had their inventory change from week 2 to week 3? 
``` sql
WITH CHANGED_INVENTORY AS (SELECT *
                           FROM PRODUCTS_SNAPSHOT
                           WHERE DATE_TRUNC(DAY, DBT_VALID_TO) = '2024-4-8'
                              OR DATE_TRUNC(DAY, DBT_VALID_FROM) = '2024-4-8')
SELECT PRODUCT_ID,
       OLD.INVENTORY AS OLD_INVENTORY,
       NEW.INVENTORY AS NEW_INVENTORY
FROM CHANGED_INVENTORY AS OLD
         JOIN CHANGED_INVENTORY AS NEW USING (PRODUCT_ID)
WHERE NEW.DBT_VALID_TO IS NULL
  AND OLD.DBT_VALID_TO IS NOT NULL
  AND NEW.INVENTORY <> OLD.INVENTORY;
  
--+------------------------------------+-------------+-------------+
--|PRODUCT_ID                          |OLD_INVENTORY|NEW_INVENTORY|
--+------------------------------------+-------------+-------------+
--|689fb64e-a4a2-45c5-b9f2-480c2155624d|56           |44           |
--|b66a7143-c18a-43bb-b5dc-06bb5d1d3160|89           |53           |
--|be49171b-9f72-4fc9-bf7a-9a52e259836b|64           |50           |
--|4cda01b9-62e2-46c5-830f-b7f262a58fb1|20           |0            |
--|55c6a062-5f4a-4a8b-a8e5-05ea5e6715a3|25           |15           |
--|fb0e8be7-5ac4-4a76-a1fa-2cc4bf0b2d80|10           |0            |
--+------------------------------------+-------------+-------------+
```
