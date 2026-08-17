{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

select
    t.id as transaction_id,
    t.account_id,
    a.customer_id,
    t.txn_type,
    t.amount,
    t.related_account_id,
    t.status,
    t.created_at,
    t.updated_at
from {{ ref('stg_transactions') }} t
left join {{ ref('dim_accounts') }} a
    on t.account_id = a.account_id
    and a.is_current = true

{% if is_incremental() %}
where t.id > (select coalesce(max(transaction_id), 0) from {{ this }})
{% endif %}