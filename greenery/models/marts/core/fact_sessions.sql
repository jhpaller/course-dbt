
WITH SESSIONS AS (SELECT * FROM {{ ref('int_sessions')}})
SELECT SESSION_ID,
       USER_ID,
       ORDER_ID,
       SESSION_START,
       SESSION_END,
       {{ row_number('USER_ID', 'SESSION_START', 'ASC') }}  AS SESSION_SEQ_ASC,
       {{ row_number('USER_ID', 'SESSION_START', 'DESC') }} AS SESSION_SEQ_DESC
FROM SESSIONS