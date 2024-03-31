
WITH ORDER_ITEMS AS (SELECT * FROM {{ ref('stg_postgres__order_items')}}),
     PRODUCTS AS (SELECT * FROM {{ ref('stg_postgres__products')}})
SELECT ORDER_ID,
       PRODUCT_ID,
       PRICE            AS UNIT_PRICE,
       QUANTITY,
       PRICE * QUANTITY AS ORDER_PRICE
FROM ORDER_ITEMS
         JOIN PRODUCTS USING (PRODUCT_ID)
