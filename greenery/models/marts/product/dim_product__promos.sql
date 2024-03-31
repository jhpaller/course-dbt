
WITH PROMOS AS (SELECT * FROM {{ ref('stg_postgres__promos')}})
SELECT PROMO_ID,
       DISCOUNT,
       STATUS
FROM PROMOS
