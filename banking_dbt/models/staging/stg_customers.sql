with ranked as (

    select
        id,
        first_name,
        last_name,
        email,
        status,
        try_to_timestamp_ntz(created_at) as created_at,
        try_to_timestamp_ntz(updated_at) as updated_at,
        _op,
        _source_ts_ms,
        _lsn,
        row_number() over (
            partition by id
            order by _source_ts_ms desc, _lsn desc
        ) as rn
    from {{ source('raw', 'customers') }}

)

select
    id,
    first_name,
    last_name,
    email,
    status,
    created_at,
    updated_at,
    _op as last_operation,
    _source_ts_ms as source_ts_ms,
    _lsn as lsn
from ranked
where rn = 1
  and _op != 'd'