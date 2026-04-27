-- =============================================================================
-- Export for ca-geo-weather-daily: starting_point_id, submarket_id, sp_name
-- Join: geo_intelligence.public.maindb_starting_point
-- Run in Mode/Snowflake, save as starting_point_to_submarket.csv in data/
-- Match column names to what pay_gap.py expects (see that module header).
-- =============================================================================
--
-- Common columns on maindb_starting_point (adjust to your org's schema):
--   id / starting_point_id, name, submarket_id
-- =============================================================================

select
  sp.id as starting_point_id
, sp.name as sp_name
, sp.submarket_id
from geo_intelligence.public.maindb_starting_point sp
where sp.submarket_id in (
  select distinct v.submarket_id
  from proddb.static.view_earning_standard_submarkets v
  where v.earning_standard_market = 'CA'
)
order by 3, 1
;
