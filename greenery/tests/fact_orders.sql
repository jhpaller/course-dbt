
SELECT *
FROM {{ref('fact_orders')}}
WHERE ESTIMATED_DELIVERY_AT < CREATED_AT
   OR DELIVERED_AT < CREATED_AT