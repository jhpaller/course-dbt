
-- remove PII
select
    {{ dbt_utils.star(from=ref('stg_postgres__users'), except=["FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE_NUMBER"]) }}
from {{ ref('stg_postgres__users') }}