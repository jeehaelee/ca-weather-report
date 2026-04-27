-- =============================================================================
-- Real submarket_id + name + geo_key for data/submarket_region_map.csv
--
-- 1) `fact_region` (or your region bridge) is the SOT for which submarket
--    belongs in which market — point `region_from_fr` at the real object.
-- 2) `geo_key` must match a key in ca-geo-weather-daily/data/geo_centroids.json
-- =============================================================================
--
-- PART A — sanity check: list all CA ESM submarket IDs and names (run in Mode)
--   Export to CSV and merge a geo_key column in Sheets/Excel, OR use PART B.
-- =============================================================================

select
  sm.id as submarket_id,
  sm.name as submarket_name
from geo_intelligence.public.maindb_submarket sm
inner join (
  select distinct v.submarket_id
  from proddb.static.view_earning_standard_submarkets v
  where v.earning_standard_market = 'CA'
) ca
  on ca.submarket_id = sm.id
order by 1
;

-- =============================================================================
-- PART B — production: wire `region_from_fr` to your `fact_region`, then run
--   this SELECT and export three columns to submarket_region_map.csv
-- =============================================================================
/*
with ca_submarket as (
  select distinct v.submarket_id
  from proddb.static.view_earning_standard_submarkets v
  where v.earning_standard_market = 'CA'
),
sm as (
  select sm.id as submarket_id, sm.name as submarket_name
  from geo_intelligence.public.maindb_submarket sm
  inner join ca_submarket c on c.submarket_id = sm.id
),
region_from_fr as (
  -- TODO: replace with your `fact_region` (SOT) — one row per submarket.
  -- Must expose: submarket_id, region_label (or region_id to map in CASE).
  select
    cast(null as number) as submarket_id,
    cast(null as varchar) as region_label
  where 1 = 0
),
enriched as (
  select
    s.submarket_id,
    s.submarket_name,
    r.region_label
  from sm s
  left join region_from_fr r on r.submarket_id = s.submarket_id
)
select
  e.submarket_id,
  e.submarket_name,
  case
    when e.region_label is null then null
    when lower(e.region_label) like any ('%inland empire%') then 'inland_empire'
    when lower(e.region_label) like any ('%east bay%') then 'east_bay'
    when lower(e.region_label) like any ('%orange county%') then 'orange_county'
    when lower(e.region_label) like any ('%sacramento%') then 'sacramento'
    when lower(e.region_label) like any ('%san diego%') then 'san_diego'
    when lower(e.region_label) like any ('%south bay%') then 'south_bay'
    when lower(e.region_label) like any ('%north bay%') then 'north_bay'
    when lower(e.region_label) like any ('%peninsula%') then 'peninsula'
    when lower(e.region_label) like any ('%san francisco%') then 'san_francisco'
    when lower(e.region_label) like any ('%fresno%') then 'fresno'
    when lower(e.region_label) like any ('%monterey%') then 'monterey'
    when lower(e.region_label) like any ('%bakersfield%') then 'bakersfield'
    when lower(e.region_label) like any ('%eureka%') or lower(e.region_label) like any ('%arcata%') then 'eureka_arcata'
    when lower(e.region_label) like any ('%hollister%') then 'hollister'
    when lower(e.region_label) like any ('%yucca%') then 'yucca_valley'
    when lower(e.region_label) like any ('%oroville%') then 'oroville'
    when lower(e.region_label) like any ('%imperial%') then 'imperial_valley'
    when lower(e.region_label) like any ('%los banos%') then 'los_banos'
    when lower(e.region_label) like any ('%glenbrook%') then 'glenbrook'
    when lower(e.region_label) like any ('%tahoe%') then 'lake_tahoe'
    when lower(e.region_label) like any ('%ridgecrest%') then 'ridgecrest'
    when lower(e.region_label) like any ('%red bluff%') then 'red_bluff'
    when lower(e.region_label) like any ('%delano%') then 'delano'
    when lower(e.region_label) like any ('%crescent city%') then 'crescent_city'
    when lower(e.region_label) like any ('%sonora%') then 'sonora'
    when lower(e.region_label) like any ('%patterson%') then 'patterson'
    when lower(e.region_label) like any ('%barstow%') then 'barstow'
    when lower(e.region_label) like any ('%wasco%') or lower(e.region_label) like any ('%shafter%') then 'wasco_shafter'
    when lower(e.region_label) like any ('%tehachapi%') then 'tehachapi'
    when lower(e.region_label) like any ('%la valley%') or lower(e.region_label) like '%l.a. valley%' then 'la_valley'
    when lower(e.region_label) like any ('%los angeles%') and lower(e.region_label) not like any ('%inland%','%valley%') then 'los_angeles'
    when lower(e.region_label) like any ('%central coast%') then 'central_coast'
    when lower(e.region_label) like any ('%central valley%') then 'central_valley'
  end as geo_key
from enriched e
where geo_key is not null
order by 1
;
*/
