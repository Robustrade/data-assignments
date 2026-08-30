-- Mirrors the source CHECK on payment_attempt.
select
    payment_id,
    attempt_no,
    result,
    failure_code
from {{ ref("payment_attempt_current") }}
where (result = 'SUCCESS' and failure_code is not null)
   or (result <> 'SUCCESS' and failure_code is null)
