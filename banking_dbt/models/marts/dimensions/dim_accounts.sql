select
    id as account_id,
    customer_id,
    account_type,
    balance,
    currency,
    status,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to = '9999-12-31') as is_current,
    dbt_is_deleted as is_deleted
from {{ ref('accounts_snapshot') }}