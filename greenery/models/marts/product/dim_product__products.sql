
WITH PRODUCTS AS (SELECT * FROM {{ ref('stg_postgres__products')}})
SELECT PRODUCT_ID,
       NAME,
       PRICE,
       INVENTORY
FROM PRODUCTS;
