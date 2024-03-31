
WITH EVENTS AS (SELECT * FROM {{ ref('stg_postgres__events')}})
SELECT SESSION_ID,
       PRODUCT_ID,
       COUNT_IF(EVENT_TYPE = 'page_view')   AS PAGE_VIEW_COUNT,
       COUNT_IF(EVENT_TYPE = 'add_to_cart') AS ADD_TO_CART_COUNT,
       MIN(CREATED_AT)                      AS SESSION_WINDOW_START,
       MAX(CREATED_AT)                      AS SESSION_WINDOW_END
FROM EVENTS
GROUP BY SESSION_ID, PRODUCT_ID;
