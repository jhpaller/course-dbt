
WITH EVENTS AS (SELECT * FROM {{ ref('stg_postgres__events')}})
SELECT SESSION_ID,
       MIN(USER_ID)             AS USER_ID,
       MIN(ORDER_ID)            AS ORDER_ID,
       COUNT(DISTINCT USER_ID)  AS USER_PER_SESSION,  -- add check not greater than 1
       COUNT(DISTINCT ORDER_ID) AS ORDER_PER_SESSION, -- add check not more than 1
       MIN(CREATED_AT)          AS SESSION_START,
       MAX(CREATED_AT)          AS SESSION_END
FROM EVENTS
GROUP BY SESSION_ID;
