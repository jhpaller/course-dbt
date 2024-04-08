
WITH ORDERS AS (SELECT * FROM {{ ref('stg_postgres__orders')}}),
     PROMOS AS (SELECT * FROM {{ ref('stg_postgres__promos')}})
SELECT ORDERS.ORDER_ID,
       ORDERS.USER_ID,
       ORDERS.PROMO_ID,
       ORDERS.ADDRESS_ID,
       ORDERS.CREATED_AT,
       ORDERS.ORDER_COST,
       IFNULL(PROMOS.DISCOUNT, 0) / ORDERS.ORDER_COST   AS PERCENT_DISCOUNT,
       PROMOS.DISCOUNT                                  AS ORDER_DISCOUNT,
       ORDERS.SHIPPING_COST,
       ORDERS.ORDER_TOTAL,
       ORDERS.TRACKING_ID,
       ORDERS.SHIPPING_SERVICE,
       ORDERS.ESTIMATED_DELIVERY_AT,
       ORDERS.DELIVERED_AT,
       ORDERS.STATUS,
       IFF(ORDERS.STATUS = 'delivered',
           DATEDIFF(DAY, ESTIMATED_DELIVERY_AT, DELIVERED_AT),
           NULL)                                        AS DELIVERY_DAYS_PAST_ESTIMATE,
       {{ row_number('USER_ID', 'CREATED_AT', TRUE) }}  AS ORDER_SEQ_ASC,
       {{ row_number('USER_ID', 'CREATED_AT', FALSE) }} AS ORDER_SEQ_DESC
       --ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY CREATED_AT ASC)  AS ORDER_SEQ_ASC,
       --ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY CREATED_AT DESC) AS ORDER_SEQ_DESC
FROM ORDERS
         LEFT JOIN PROMOS USING (PROMO_ID)