
select
    {{ dbt_utils.star(from=ref('stg_postgres__promos')) }}
from {{ ref('stg_postgres__promos') }}
