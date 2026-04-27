-- =============================================================================
-- Export submarket_id + name + geo_key for ca-geo-weather-daily
--
-- **Source of truth:** `fact_region` (or the table your org uses for submarket ↔
-- region). Map each submarket to exactly one `geo_key` that exists in
-- `data/geo_centroids.json` (e.g. los_angeles, san_francisco, eureka_arcata, ...).
-- =============================================================================
--
-- Use `fact_region` as the SOT to map submarket to region, then map region to `geo_key`
-- in a CASE or a small maintainable mapping table.
--
-- Example pattern (pseudocode — replace with your real fact_region / dimension names):
--
-- select
--     fr.submarket_id,
--     fr.submarket_name,  -- or join maindb_submarket.name
--     case
--         when fr.region_name ilike '%Los Angeles%' and fr.region_name not ilike '%Valley%' then 'los_angeles'
--         when fr.region_name ilike '%Inland Empire%' then 'inland_empire'
--         -- ... one branch per geo_key
--         else null
--     end as geo_key
-- from <your_schema>.fact_region fr
-- where <ca filter>
--   and geo_key is not null
-- ;
--
-- Then save as CSV: submarket_id,submarket_name,geo_key
-- and replace ca-geo-weather-daily/data/submarket_region_map.csv
--
-- Canonical submarket display names (optional, if not on fact_region):
--   join geo_intelligence.public.maindb_submarket sm
--     on sm.id = fr.submarket_id
--   , sm.name as submarket_name
-- =============================================================================

select
  'TODO_SUBMARKET_ID' as submarket_id
, 'TODO' as submarket_name
, 'los_angeles' as geo_key
where 1 = 0;
