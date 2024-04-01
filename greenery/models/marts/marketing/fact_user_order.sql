

WITH SESSIONS AS (SELECT * FROM {{ ref('fact_sessions')}}),
     ORDERS AS (SELECT * FROM {{ ref('fact_orders')}}),
     ORDER_PRODUCTS AS (SELECT * FROM {{ ref('int_order_products')}}),
     USER_SESSION_STATS AS (SELECT USER_ID,
                                   MIN(SESSION_START)                                              AS FIRST_SESSION_DATE,
                                   MAX(SESSION_END)                                                AS LATEST_SESSION_DATE,
                                   COUNT(*)                                                        AS SESSION_COUNT,
                                   MIN(CASE SESSION_SEQ_ASC WHEN 1 THEN ORDER_ID IS NOT NULL END)  AS FIRST_SESSION_HAD_ORDER,
                                   MIN(CASE SESSION_SEQ_DESC WHEN 1 THEN ORDER_ID IS NOT NULL END) AS LATEST_SESSION_HAD_ORDER
                            FROM SESSIONS
                            GROUP BY USER_ID),
     ORDER_PRODUCT_STATS AS (SELECT ORDER_ID,
                                    COUNT(*)      AS PRODUCT_COUNT,
                                    SUM(QUANTITY) AS ITEM_COUNT
                             FROM ORDER_PRODUCTS
                             GROUP BY ORDER_ID),
     USER_ORDER_STATS AS (SELECT USER_ID,
                                 COUNT(*)                                                             AS ORDER_COUNT,
                                 MIN(CREATED_AT)                                                      AS FIRST_ORDER_DATE,
                                 MAX(CREATED_AT)                                                      AS LATEST_ORDER_DATE,
                                 -- order cost comparison
                                 MIN(CASE ORDER_SEQ_ASC WHEN 1 THEN ORDER_COST END)                   AS FIRST_ORDER_COST,
                                 MIN(ORDER_COST)                                                      AS MIN_ORDER_COST,
                                 AVG(ORDER_COST)                                                      AS AVG_ORDER_COST,
                                 MAX(ORDER_COST)                                                      AS MAX_ORDER_COST,
                                 MAX(CASE ORDER_SEQ_DESC WHEN 1 THEN ORDER_COST END)                  AS LATEST_ORDER_COST,
                                 -- cumulative cost & savings
                                 SUM(ORDER_COST)                                                      AS SUM_ORDER_COST,
                                 SUM(ORDER_DISCOUNT)                                                  AS SUM_ORDER_DISCOUNT,
                                 -- order percent discount comparison
                                 MIN(CASE ORDER_SEQ_ASC WHEN 1 THEN PERCENT_DISCOUNT END)             AS FIRST_ORDER_PERCENT_DISCOUNT,
                                 MIN(PERCENT_DISCOUNT)                                                AS MIN_ORDER_PERCENT_DISCOUNT,
                                 AVG(PERCENT_DISCOUNT)                                                AS AVG_ORDER_PERCENT_DISCOUNT,
                                 MAX(PERCENT_DISCOUNT)                                                AS MAX_ORDER_PERCENT_DISCOUNT,
                                 MAX(CASE ORDER_SEQ_DESC WHEN 1 THEN PERCENT_DISCOUNT END)            AS LATEST_ORDER_PERCENT_DISCOUNT,
                                 -- order delivery vs estimate comparison
                                 MIN(CASE ORDER_SEQ_ASC WHEN 1 THEN DELIVERY_DAYS_PAST_ESTIMATE END)  AS FIRST_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
                                 MIN(DELIVERY_DAYS_PAST_ESTIMATE)                                     AS MIN_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
                                 AVG(DELIVERY_DAYS_PAST_ESTIMATE)                                     AS AVG_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
                                 MAX(DELIVERY_DAYS_PAST_ESTIMATE)                                     AS MAX_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
                                 MAX(CASE ORDER_SEQ_DESC WHEN 1 THEN DELIVERY_DAYS_PAST_ESTIMATE END) AS LATEST_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
                                 -- order product count comparison
                                 MIN(CASE ORDER_SEQ_ASC WHEN 1 THEN PRODUCT_COUNT END)                AS FIRST_ORDER_PRODUCT_COUNT,
                                 MIN(PRODUCT_COUNT)                                                   AS MIN_ORDER_PRODUCT_COUNT,
                                 AVG(PRODUCT_COUNT)                                                   AS AVG_ORDER_PRODUCT_COUNT,
                                 MAX(PRODUCT_COUNT)                                                   AS MAX_ORDER_PRODUCT_COUNT,
                                 MAX(CASE ORDER_SEQ_DESC WHEN 1 THEN PRODUCT_COUNT END)               AS LATEST_ORDER_PRODUCT_COUNT
                          FROM ORDERS
                                   JOIN ORDER_PRODUCT_STATS USING (ORDER_ID)
                          GROUP BY USER_ID)
SELECT USER_ID,
       FIRST_SESSION_DATE,
       LATEST_SESSION_DATE,
       FIRST_SESSION_HAD_ORDER,
       LATEST_SESSION_HAD_ORDER,
       SESSION_COUNT,
       IFNULL(ORDER_COUNT, 0) AS ORDER_COUNT,
       FIRST_ORDER_DATE,
       LATEST_ORDER_DATE,
       FIRST_ORDER_COST,
       MIN_ORDER_COST,
       AVG_ORDER_COST,
       MAX_ORDER_COST,
       LATEST_ORDER_COST,
       SUM_ORDER_COST,
       SUM_ORDER_DISCOUNT,
       FIRST_ORDER_PERCENT_DISCOUNT,
       MIN_ORDER_PERCENT_DISCOUNT,
       AVG_ORDER_PERCENT_DISCOUNT,
       MAX_ORDER_PERCENT_DISCOUNT,
       LATEST_ORDER_PERCENT_DISCOUNT,
       FIRST_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
       MIN_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
       AVG_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
       MAX_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
       LATEST_ORDER_DELIVERY_PAST_ESTIMATE_DAYS,
       FIRST_ORDER_PRODUCT_COUNT,
       MIN_ORDER_PRODUCT_COUNT,
       AVG_ORDER_PRODUCT_COUNT,
       MAX_ORDER_PRODUCT_COUNT,
       LATEST_ORDER_PRODUCT_COUNT
FROM USER_SESSION_STATS
         LEFT JOIN USER_ORDER_STATS USING (USER_ID)
