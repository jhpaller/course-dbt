
select
    {{ dbt_utils.star(from=ref('stg_postgres__products')) }}
from {{ ref('stg_postgres__products') }}
