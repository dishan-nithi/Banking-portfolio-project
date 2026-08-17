select
    id as customer_id,
    first_name,
    last_name,
    email,
    status,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to = '9999-12-31') as is_current,
    dbt_is_deleted as is_deleted
from {{ ref('customers_snapshot') }}