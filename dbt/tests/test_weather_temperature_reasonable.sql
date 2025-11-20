/*
Custom Test: test_weather_temperature_reasonable
Description: Ensure weather temperatures are within reasonable range (-50°C to 60°C)

This test fails if temperatures are outside reasonable bounds.
*/

select
    date,
    outlet_id,
    avg_temperature,
    min_temperature,
    max_temperature
from {{ ref('fct_daily_business_performance') }}
where avg_temperature is not null
  and (
      avg_temperature < -50
      or avg_temperature > 60
      or min_temperature < -50
      or max_temperature > 60
  )
