with ranked as (

    select
        id,
        customer_id,
        account_type,
        try_to_decimal(balance, 18, 2) as balance,
        currency,
        status,
        try_to_timestamp_ntz(created_at) as created_at,
        try_to_timestamp_ntz(updated_at) as updated_at,
        try_to_timestamp_ntz(closed_at) as closed_at,
        _op,
        _source_ts_ms,
        _lsn,
        row_number() over (
            partition by id
            order by _source_ts_ms desc, _lsn desc
        ) as rn
    from {{ source('raw', 'accounts') }}

)

select
    id,
    customer_id,
    account_type,
    balance,
    currency,
    status,
    created_at,
    updated_at,
    closed_at,
    _op as last_operation,
    _source_ts_ms as source_ts_ms,
    _lsn as lsn
from ranked
where rn = 1
  and _op != 'd'