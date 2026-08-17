select *
from {{ ref('rpt_balance_reconciliation') }}
where abs(discrepancy) > 0.01