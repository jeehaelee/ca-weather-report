-- =============================================================================
-- Quick spike diagnosis: CA alert SMs, Rx-only, Pacific local hour/day.
-- Use after you see elevated Rx cancels ~7am PT (adjust hour window below).
--
-- Run 1) geography + merchant concentration
-- Run 2) SHOW COLUMNS on pivot, then add real cancel_* / unassign_* columns to GROUP BY
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0) List pivot columns that might explain cancels (run once; paste names into §2)
-- -----------------------------------------------------------------------------
-- show columns from datalake.edw_logistics.vw_fact_fulfillment_attributes_pivot;

-- -----------------------------------------------------------------------------
-- 1) Rx cancels by PT hour + submarket (which SM is driving the spike?)
-- -----------------------------------------------------------------------------
with ca_sm (id) as (
  select * from (values (10), (16), (39), (87), (37), (1), (3)) as t (id)
),
stores_in_scope as (
  select s.store_id
  from datalake.edw.dimension_store s
  join snowflake."edw.cng".dimension_new_vertical_store_tags nvs
    on nvs.store_id = s.store_id
   and cast(nvs.is_filtered_mp_vertical as integer) = 1
  where cast(s.country_id as integer) = 1
    and s.nv_org != 'drive'
    and s.nv_business_line in (
      'Grocery', '3P Convenience', 'Alcohol', 'Pets',
      'Active & Office', 'Home & Wellness', 'Flowers'
    )
    and s.is_test = 0
),
drive as (
  select s.store_id
  from datalake.edw.dimension_store s
  where cast(s.country_id as integer) = 1
    and s.nv_org = 'drive'
    and s.nv_business_line not in (
      'Grocery', '3P Convenience', 'Alcohol', 'Pets',
      'Active & Office', 'Home & Wellness', 'Flowers'
    )
    and s.is_test = 0
),
rx_ca as (
  select
    cast(at_timezone(c.created_at_time_utc, 'America/Los_Angeles') as date) as local_date_pt,
    extract(hour from at_timezone(c.created_at_time_utc, 'America/Los_Angeles')) as local_hour_pt,
    cast(c.submarket_id as bigint) as submarket_id,
    c.store_id,
    cast(c.is_cancelled as integer) as is_cancelled
  from datalake.edw_logistics.vw_fact_fulfillment_attributes_pivot c
  inner join ca_sm sm on cast(c.submarket_id as bigint) = sm.id
  left join stores_in_scope nv on nv.store_id = c.store_id
  left join drive drv on drv.store_id = c.store_id
  where cast(c.country_id as integer) = 1
    and c.fulfillment_type = 'dasher'
    and nv.store_id is null
    and drv.store_id is null
    and cast(at_timezone(c.created_at_time_utc, 'America/Los_Angeles') as date)
        >= cast(date_trunc('day', at_timezone(current_timestamp, 'America/Los_Angeles')) as date)
           - interval '1' day
    and at_timezone(c.created_at_time_utc, 'America/Los_Angeles')
        <= at_timezone(current_timestamp, 'America/Los_Angeles')
    -- ~6–10am PT window; tighten to (7) for exactly 7am hour
    and extract(hour from at_timezone(c.created_at_time_utc, 'America/Los_Angeles')) between 6 and 10
)
select
  local_date_pt,
  local_hour_pt,
  submarket_id,
  count(*) as deliveries,
  sum(case when is_cancelled = 1 then 1 else 0 end) as cxls,
  cast(sum(case when is_cancelled = 1 then 1 else 0 end) as double)
    / nullif(cast(count(*) as double), 0) as cxl_rate
from rx_ca
group by 1, 2, 3
having sum(case when is_cancelled = 1 then 1 else 0 end) > 0
order by local_date_pt desc, cxl_rate desc, cxls desc;

-- -----------------------------------------------------------------------------
-- 2) Same slice: top stores by cancel count (run as a second statement)
-- -----------------------------------------------------------------------------
with ca_sm (id) as (
  select * from (values (10), (16), (39), (87), (37), (1), (3)) as t (id)
),
stores_in_scope as (
  select s.store_id
  from datalake.edw.dimension_store s
  join snowflake."edw.cng".dimension_new_vertical_store_tags nvs
    on nvs.store_id = s.store_id
   and cast(nvs.is_filtered_mp_vertical as integer) = 1
  where cast(s.country_id as integer) = 1
    and s.nv_org != 'drive'
    and s.nv_business_line in (
      'Grocery', '3P Convenience', 'Alcohol', 'Pets',
      'Active & Office', 'Home & Wellness', 'Flowers'
    )
    and s.is_test = 0
),
drive as (
  select s.store_id
  from datalake.edw.dimension_store s
  where cast(s.country_id as integer) = 1
    and s.nv_org = 'drive'
    and s.nv_business_line not in (
      'Grocery', '3P Convenience', 'Alcohol', 'Pets',
      'Active & Office', 'Home & Wellness', 'Flowers'
    )
    and s.is_test = 0
),
rx_ca as (
  select
    cast(at_timezone(c.created_at_time_utc, 'America/Los_Angeles') as date) as local_date_pt,
    extract(hour from at_timezone(c.created_at_time_utc, 'America/Los_Angeles')) as local_hour_pt,
    cast(c.submarket_id as bigint) as submarket_id,
    c.store_id,
    cast(c.is_cancelled as integer) as is_cancelled
  from datalake.edw_logistics.vw_fact_fulfillment_attributes_pivot c
  inner join ca_sm sm on cast(c.submarket_id as bigint) = sm.id
  left join stores_in_scope nv on nv.store_id = c.store_id
  left join drive drv on drv.store_id = c.store_id
  where cast(c.country_id as integer) = 1
    and c.fulfillment_type = 'dasher'
    and nv.store_id is null
    and drv.store_id is null
    and cast(at_timezone(c.created_at_time_utc, 'America/Los_Angeles') as date)
        >= cast(date_trunc('day', at_timezone(current_timestamp, 'America/Los_Angeles')) as date)
           - interval '1' day
    and at_timezone(c.created_at_time_utc, 'America/Los_Angeles')
        <= at_timezone(current_timestamp, 'America/Los_Angeles')
    and extract(hour from at_timezone(c.created_at_time_utc, 'America/Los_Angeles')) between 6 and 10
)
select
  local_date_pt,
  local_hour_pt,
  submarket_id,
  store_id,
  sum(case when is_cancelled = 1 then 1 else 0 end) as cxls
from rx_ca
where is_cancelled = 1
group by 1, 2, 3, 4
order by local_date_pt desc, cxls desc
limit 80;

-- -----------------------------------------------------------------------------
-- 3) After SHOW COLUMNS: add the real attribution column(s), e.g.:
--    coalesce(c.cancel_reason, c.cancellation_reason, 'UNKNOWN') as cancel_dim
-- group by local_date_pt, local_hour_pt, submarket_id, cancel_dim
-- -----------------------------------------------------------------------------
