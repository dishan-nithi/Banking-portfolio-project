with transaction_effects as (

    select
        account_id,
        case txn_type
            when 'DEPOSIT' then amount
            when 'WITHDRAWAL' then -amount
            when 'TRANSFER' then -amount
        end as effect
    from {{ ref('fact_transactions') }}

    union all

    select
        related_account_id as account_id,
        amount as effect
    from {{ ref('fact_transactions') }}
    where txn_type = 'TRANSFER' and related_account_id is not null

),

computed_balances as (
    select account_id, sum(effect) as computed_balance
    from transaction_effects
    group by account_id
)

select
    a.account_id,
    a.balance as recorded_balance,
    coalesce(cb.computed_balance, 0) as computed_balance,
    a.balance - coalesce(cb.computed_balance, 0) as discrepancy
from {{ ref('dim_accounts') }} a
left join computed_balances cb on cb.account_id = a.account_id
where a.is_current = true