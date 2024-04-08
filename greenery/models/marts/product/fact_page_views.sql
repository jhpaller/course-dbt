
WITH EVENTS AS (SELECT * FROM {{ ref('stg_postgres__events')}}),
     ORDER_ITEMS AS (SELECT * FROM {{ ref('stg_postgres__order_items')}}),
     SESSIONS AS (SELECT * FROM {{ ref('int_sessions')}})

{% set event_types = [ 'page_view', 'add_to_cart', 'checkout', 'package_shipped' ] %}

SELECT SESSION_ID,
       SESSIONS.USER_ID,
       IFNULL(EVENTS.PRODUCT_ID, ORDER_ITEMS.PRODUCT_ID) AS PRODUCT_ID,
       SESSION_START,
       SESSION_END,
       {% for event_type in event_types %}
       {{ sum_of('EVENT_TYPE', event_type) }}            AS {{ event_type }}_count,
       {% endfor %}
       DATEDIFF(MIN, SESSION_START, SESSION_END)         AS SESSION_LENGTH_MINUTES
FROM EVENTS
         LEFT JOIN ORDER_ITEMS USING (ORDER_ID)
         JOIN SESSIONS USING (SESSION_ID)
{{ dbt_utils.group_by(n=5) }}
