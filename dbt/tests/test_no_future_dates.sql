/*
Custom Test: test_no_future_dates
Description: Ensure no dates in the future exist in our fact table

This test fails if any dates are beyond today's date.
*/

select
    date,
    count(*) as record_count
from {{ ref('fct_daily_business_performance') }}
where date > current_date
group by date
having count(*) > 0
