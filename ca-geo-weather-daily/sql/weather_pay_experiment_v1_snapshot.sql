-- =============================================================================
-- Daily snapshot: pay already configured in WEATHER_PAY_EXPERIMENT_V1
-- (same shape as the ops team's query; export result as CSV for the Python
--  pay-gap pass — file: data/weather_pay_experiment_snapshot.csv)
-- =============================================================================
-- Notes:
-- - run_date = latest batch is used here; align export time with the weather report.
-- - Rows represent SP-level pay intent; weather_opt_final populated => treated as
--   "has pay" for (starting_point, forecast_date, daypart) when matching the report.
-- =============================================================================

select
  v.forecast_date
, v.run_date
, v.starting_point_id
, v.sp_name
, v.submarket_id
, v.sm_name
, v.daypart
, v.local_start_time
, v.local_end_time
, v.start_time_window as local_start_hour
, v.end_time_window as local_end_hour
, v.weather_opt_final
, v.add_weather_pay
, v.rain_snow_tag
, v.run_tag
from proddb.static.weather_pay_experiment_v1 v
left join proddb.static.view_earning_standard_submarkets sm
  on sm.submarket_id = v.submarket_id
where 1 = 1
  and sm.earning_standard_market = 'CA'
  and v.run_date = (select max(run_date) from proddb.static.weather_pay_experiment_v1)
  and v.add_weather_pay = 'yes'
order by v.forecast_date desc, v.submarket_id, v.starting_point_id, v.daypart
;
