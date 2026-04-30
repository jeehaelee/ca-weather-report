-- Live vs LW + vs yesterday: cancellation + late20m by NY hour and vertical.
-- Vert: NV | <pick_model> (stores_in_scope), Drive (drive CTE), else Rx.
-- historical: dimension_deliveries, NY-local created_at date; 21-day scan for joins.
-- live: vw_fact_fulfillment_attributes_pivot; yesterday + today NY (>= today - 1 day).
-- Cancel "why": cancellation_reason_category from pivot (UNKNOWN if null). cxl_share_of_vert_hour = slice of cancels in that vert×hour.
-- Geo: Large SM Degradation CA set — submarket_id 10,16,39,87,37,1,3 (East Bay, SD, North Bay, Fresno, South Bay, LA, OC).
-- Trino: current_timestamp without ()
-- If live fails on column: confirm pivot has submarket_id (else join dimension_store on store_id and filter store.submarket_id).

with ca_sm (id) as (
  select * from (values (10), (16), (39), (87), (37), (1), (3)) as t (id)
),

stores_in_scope as (
  select
    s.store_id,
    s.business_id,
    s.business_name,
    s.nv_business_line,
    nvs.pick_model,
    case
      when s.nv_business_line in ('Pets', 'Active & Office', 'Home & Wellness', 'Flowers')
      then 'Retail'
      else s.nv_business_line
    end as vertical
  from datalake.edw.dimension_store s
  join snowflake."edw.cng".dimension_new_vertical_store_tags nvs
    on nvs.store_id = s.store_id
   and cast(nvs.is_filtered_mp_vertical as integer) = 1
  where cast(s.country_id as integer) = 1
    and s.nv_org != 'drive'
    and s.nv_business_line in (
      'Grocery',
      '3P Convenience',
      'Alcohol',
      'Pets',
      'Active & Office',
      'Home & Wellness',
      'Flowers'
    )
    and s.is_test = 0
),

drive as (
  select
    s.store_id,
    s.business_id,
    s.business_name,
    s.nv_business_line
  from datalake.edw.dimension_store s
  where cast(s.country_id as integer) = 1
    and s.nv_org = 'drive'
    and s.nv_business_line not in (
      'Grocery',
      '3P Convenience',
      'Alcohol',
      'Pets',
      'Active & Office',
      'Home & Wellness',
      'Flowers'
    )
    and s.is_test = 0
),

historical as (
  select
    cast(date_trunc('day', at_timezone(dd.created_at, 'America/New_York')) as date) as dte,
    case
      when nv.store_id is not null then 'NV | ' || nv.pick_model
      when drv.store_id is not null then 'Drive'
      else 'Rx'
    end as vert,
    extract(hour from at_timezone(dd.created_at, 'America/New_York')) as local_hour,
    count(distinct case when dd.cancelled_at is not null then dd.delivery_id end) as cxls,
    count(distinct dd.delivery_id) as delivs,
    count(distinct case
      when date_diff('second', dd.quoted_delivery_time, dd.actual_delivery_time) > 1200
      then dd.delivery_id
    end) as late20m,
    cast(count(distinct case when dd.cancelled_at is not null then dd.delivery_id end) as double)
      / nullif(cast(count(distinct dd.delivery_id) as double), 0) as pct_cxled,
    cast(count(distinct case
      when date_diff('second', dd.quoted_delivery_time, dd.actual_delivery_time) > 1200
      then dd.delivery_id
    end) as double)
      / nullif(cast(count(distinct dd.delivery_id) as double), 0) as pct_late20m
  from datalake.proddb.dimension_deliveries dd
  inner join ca_sm sm
    on cast(dd.submarket_id as bigint) = sm.id
  left join stores_in_scope nv
    on nv.store_id = dd.store_id
  left join drive drv
    on drv.store_id = dd.store_id
  where dd.parent_delivery_id is null
    and dd.active_date is not null
    and dd.is_from_store_to_us = false
    and dd.is_test = false
    and dd.return_order_info is null
    and dd.is_consumer_pickup = false
    and coalesce(dd.fulfillment_type, '') not in ('merchant_fleet', 'shipping')
    and cast(at_timezone(dd.created_at, 'America/New_York') as date)
        >= cast(date_trunc('day', at_timezone(current_timestamp, 'America/New_York')) as date)
           - interval '21' day
  group by 1, 2, 3
),

live_delivs as (
  with data as (
    select
      c.*,
      at_timezone(c.created_at_time_utc, 'America/New_York') as local_ts,
      at_timezone(c.cancelled_at_time_utc, 'America/New_York') as local_cancel_ts,
      cast(at_timezone(c.created_at_time_utc, 'America/New_York') as date) as local_date,
      extract(hour from at_timezone(c.created_at_time_utc, 'America/New_York')) as local_hour,
      nv.business_name,
      nv.nv_business_line,
      case
        when nv.store_id is not null then 'NV | ' || nv.pick_model
        when drv.store_id is not null then 'Drive'
        else 'Rx'
      end as vert,
      nv.pick_model
    from datalake.edw_logistics.vw_fact_fulfillment_attributes_pivot c
    inner join ca_sm sm
      on cast(c.submarket_id as bigint) = sm.id
    left join stores_in_scope nv
      on nv.store_id = c.store_id
    left join drive drv
      on drv.store_id = c.store_id
    where cast(at_timezone(c.created_at_time_utc, 'America/New_York') as date)
          >= cast(date_trunc('day', at_timezone(current_timestamp, 'America/New_York')) as date)
             - interval '1' day
      and at_timezone(c.created_at_time_utc, 'America/New_York')
          <= at_timezone(current_timestamp, 'America/New_York')
      and cast(c.country_id as integer) = 1
      and c.fulfillment_type = 'dasher'
  )
  select
    local_date,
    vert,
    local_hour,
    coalesce(d.cancellation_reason_category, 'UNKNOWN') as cancellation_reason_category,
    count(distinct d.delivery_uuid) as volume,
    count(distinct case when cast(d.is_cancelled as integer) = 1 then d.delivery_uuid end) as cxl_vol,
    count(distinct case when cast(d.is_20_min_late as integer) = 1 then d.delivery_uuid end) as late20m_vol,
    cast(count(distinct case when cast(d.is_cancelled as integer) = 1 then d.delivery_uuid end) as double)
      / nullif(cast(count(distinct d.delivery_uuid) as double), 0) as cxl_rate,
    cast(count(distinct case when cast(d.is_20_min_late as integer) = 1 then d.delivery_uuid end) as double)
      / nullif(cast(count(distinct d.delivery_uuid) as double), 0) as pct_late20m
  from data d
  group by 1, 2, 3, 4
)

select
  r.local_date,
  r.local_hour,
  r.vert,
  r.cancellation_reason_category,
  r.cxls_lw,
  r.delivs_lw,
  r.pct_cxled_lw,
  r.late20m_lw,
  r.pct_late20m_lw,
  r.cxls_yday,
  r.delivs_yday,
  r.pct_cxled_yday,
  r.late20m_yday,
  r.pct_late20m_yday,
  r.volume,
  r.cxl_vol,
  r.cxl_rate,
  r.cxl_share_of_vert_hour,
  r.cxl_abs_delta_vs_lw,
  r.cxl_rel_delta_vs_lw,
  r.cxl_abs_delta_vs_yday,
  r.cxl_rel_delta_vs_yday,
  r.late20m_vol,
  r.pct_late20m,
  r.late20m_rel_delta_vs_lw,
  r.late20m_rel_delta_vs_yday
from (
  select
    d.local_date,
    d.local_hour,
    d.vert,
    d.cancellation_reason_category,
    lw.cxls as cxls_lw,
    lw.delivs as delivs_lw,
    lw.pct_cxled as pct_cxled_lw,
    lw.late20m as late20m_lw,
    lw.pct_late20m as pct_late20m_lw,
    yday.cxls as cxls_yday,
    yday.delivs as delivs_yday,
    yday.pct_cxled as pct_cxled_yday,
    yday.late20m as late20m_yday,
    yday.pct_late20m as pct_late20m_yday,
    d.volume,
    d.cxl_vol,
    d.cxl_rate,
    cast(d.cxl_vol as double)
      / nullif(
        sum(d.cxl_vol) over (partition by d.local_date, d.local_hour, d.vert),
        0
      ) as cxl_share_of_vert_hour,
    d.cxl_rate - lw.pct_cxled as cxl_abs_delta_vs_lw,
    (d.cxl_rate - lw.pct_cxled) / nullif(lw.pct_cxled, 0) as cxl_rel_delta_vs_lw,
    d.cxl_rate - yday.pct_cxled as cxl_abs_delta_vs_yday,
    (d.cxl_rate - yday.pct_cxled) / nullif(yday.pct_cxled, 0) as cxl_rel_delta_vs_yday,
    d.late20m_vol,
    d.pct_late20m,
    (d.pct_late20m - lw.pct_late20m) / nullif(lw.pct_late20m, 0) as late20m_rel_delta_vs_lw,
    (d.pct_late20m - yday.pct_late20m) / nullif(yday.pct_late20m, 0) as late20m_rel_delta_vs_yday
  from live_delivs d
  left join historical lw
    on cast(lw.dte as date) = cast(d.local_date as date) - interval '7' day
   and lw.vert = d.vert
   and lw.local_hour = d.local_hour
  left join historical yday
    on cast(yday.dte as date) = cast(d.local_date as date) - interval '1' day
   and yday.vert = d.vert
   and yday.local_hour = d.local_hour
) r
order by
  r.local_date,
  r.local_hour,
  r.vert,
  r.cxl_share_of_vert_hour desc nulls last,
  r.cancellation_reason_category;
