
WITH EVENTS AS (SELECT * FROM {{ ref('stg_postgres__events')}}),
     ORDER_ITEMS AS (SELECT * FROM {{ ref('stg_postgres__order_items')}}),
     SESSIONS AS (SELECT * FROM {{ ref('int_sessions')}})
SELECT SESSION_ID,
       SESSIONS.USER_ID,
       IFNULL(EVENTS.PRODUCT_ID, ORDER_ITEMS.PRODUCT_ID) AS PRODUCT_ID,
       SESSION_START,
       SESSION_END,
       SUM(DECODE(EVENT_TYPE, 'page_view', 1, 0))        AS PAGE_VIEWS,
       SUM(DECODE(EVENT_TYPE, 'add_to_cart', 1, 0))      AS ADD_TO_CART,
       SUM(DECODE(EVENT_TYPE, 'checkout', 1, 0))         AS CHECKOUT,
       SUM(DECODE(EVENT_TYPE, 'package_shipped', 1, 0))  AS PACKAGE_SHIPPED,
       DATEDIFF(MIN, SESSION_START, SESSION_END)         AS SESSION_LENGTH_MINUTES
FROM EVENTS
         LEFT JOIN ORDER_ITEMS USING (ORDER_ID)
         JOIN SESSIONS USING (SESSION_ID)
GROUP BY 1, 2, 3, 4, 5
