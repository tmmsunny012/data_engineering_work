/*
Custom Test: test_rating_range
Description: Ensure all ratings are within valid range (0-5)

This test fails if any ratings are outside the 0-5 range.
*/

select
    date,
    listing_id,
    avg_rating
from {{ ref('fct_daily_business_performance') }}
where avg_rating is not null
  and (avg_rating < 0 or avg_rating > 5)
