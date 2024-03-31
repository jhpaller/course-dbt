
WITH ADDRESSES AS (SELECT * FROM {{ ref('stg_postgres__addresses')}})
SELECT ADDRESS_ID,
       ADDRESS,
       ZIPCODE,
       STATE,
       COUNTRY
FROM ADDRESSES
