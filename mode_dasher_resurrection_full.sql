-- =============================================================================
-- Lapsed dashers: >=10 LTD, inactive 28+ days as of `cohort_asof`, 7/14/30d + pre-churn
-- No Braze/email. Outcomes = completed deliveries in [cohort_asof, cohort_asof+30d).
-- ---------------------------------------------------------------------------
-- PERF (read this if slow):
--   1) cohort_core is built first — only lapsed dashers, then all heavy facts are scoped
--      to that dasher_id set (avoids full-table joins on all dashers).
--   2) no global cfg_deliveries CTE (that scanned all history). Post-period deliveries join
--      dimension_deliveries with date + dasher in the join predicate.
--   3) Optional: throttle geography in cohort_core, e.g.
--        and dda.applied_submarket_id in (...)
--      4) Optional: for development, set a fixed past cohort_asof so dimension scans use a
--         narrower time range if tables are partition-pruned on date.
-- ---------------------------------------------------------------------------
-- "syntax error ... near '<EOF>'" = only part of the script was executed; run the full `;` statement
-- =============================================================================

-- --------------- A) Snapshot parameters ---------------
with params as (
  select
    current_date()::date as cohort_asof
  , 28 as lapsed_inactive_min_days
)

-- --------------- B) Lapsed cohort first (tight — everything downstream is scoped) ---------------
, cohort_core as (
  select
    p.cohort_asof
  , p.lapsed_inactive_min_days
  , dd.dasher_id
  , dd.first_delivery_at
  , datediff('day', dd.first_delivery_at, p.cohort_asof) as days_since_fd
  , dd.lifetime_num_deliveries_made
  , dd.last_delivery_at
  , to_date(dd.last_delivery_at) as last_delivery_date
  , case
      when to_date(dd.last_delivery_at) between dateadd('day', -30, p.cohort_asof) and p.cohort_asof
      then 1 else 0
    end as active_l30d
  , dt.tier
  , dda.applied_submarket_id
  from proddb.public.dimension_dasher dd
  cross join params p
  left join dasher_ratings_prod.public.dasher_tier dt
    on dt.dasher_id = dd.dasher_id
  left join edw.dasher.dimension_dasher_applicants dda
    on dda.dasher_id = dd.dasher_id
  where dd.first_delivery_at is not null
    and dd.lifetime_num_deliveries_made >= 10
    and to_date(dd.last_delivery_at) < dateadd('day', -p.lapsed_inactive_min_days, p.cohort_asof)
  -- and dda.applied_submarket_id in ( ... )  -- optional: limit geo if cohort is still huge
)

-- --------------- C) L28D shift rollup — only cohort dashers (~orders of magnitude less IO) ---------------
, hrs as (
  select
    dds.dasher_id
  , sum(dds.adj_shift_seconds) / nullif(3600.0 * 4, 0) as avg_weekly_hours_l28d_asof_today
  , div0(
      sum(dds.total_active_time_seconds),
      nullif(sum(dds.adj_shift_seconds), 0)
    ) as utilization_l28d_asof_today
  from edw.dasher.dasher_shifts dds
  inner join cohort_core c
    on c.dasher_id = dds.dasher_id
  cross join params p
  where date(dds.active_date) between dateadd('day', -28, p.cohort_asof) and p.cohort_asof
  group by 1
)

-- --------------- D) Submarket supply — only submarkets present in cohort ---------------
, ss as (
  select
    v.submarket_id
  , sum(v.total_hours_undersupply) / nullif(sum(v.total_hours_online_ideal), 0) as undersupply_hours_pct
  , sum(v.total_hours_oversupply) / nullif(sum(v.total_hours_online_ideal), 0) as oversupply_hours_pct
  , case
      when (sum(v.total_hours_undersupply) / nullif(sum(v.total_hours_online_ideal), 0)) <= 0.015 then 'Oversupply'
      when (sum(v.total_hours_undersupply) / nullif(sum(v.total_hours_online_ideal), 0)) >= 0.035 then 'Undersupply'
      else 'Healthy'
    end as sm_supply_status
  from edw.dasher.view_agg_supply_metrics_sp_hour v
  cross join params p
  where v.submarket_id in (select c.applied_submarket_id from cohort_core c where c.applied_submarket_id is not null)
    and v.active_date between dateadd('day', -30, p.cohort_asof) and p.cohort_asof
  group by 1
)

-- --------------- E) Product flags (CNG) — only cohort dashers ---------------
, nv as (
  select
    o.dasher_id
  , max(case when o.pick_model = 'DASHER_PICK' then 1 else 0 end) as has_done_dsd
  , max(case when o.vertical_name = 'Alcohol' then 1 else 0 end) as has_done_alcohol
  from edw.cng.fact_non_rx_orders o
  where o.dasher_id in (select c.dasher_id from cohort_core c)
  group by 1
)

, base as (
  select
    c.cohort_asof
  , c.lapsed_inactive_min_days
  , c.dasher_id
  , c.first_delivery_at, c.days_since_fd, c.lifetime_num_deliveries_made
  , c.last_delivery_at, c.last_delivery_date, c.active_l30d, c.tier
  , h.avg_weekly_hours_l28d_asof_today, h.utilization_l28d_asof_today
  , c.applied_submarket_id, ss.sm_supply_status
  , coalesce(nv.has_done_dsd, 0) as has_done_dsd
  , coalesce(nv.has_done_alcohol, 0) as has_done_alcohol
  , case
      when c.lifetime_num_deliveries_made between 1 and 10  then '1-10'
      when c.lifetime_num_deliveries_made between 11 and 25  then '11-25'
      when c.lifetime_num_deliveries_made between 26 and 50  then '26-50'
      when c.lifetime_num_deliveries_made between 51 and 100 then '51-100'
      when c.lifetime_num_deliveries_made between 101 and 500 then '101-500'
      when c.lifetime_num_deliveries_made > 500 then '501+'
      else 'unknown'
    end as ltd_bucket
  from cohort_core c
  left join hrs h on h.dasher_id = c.dasher_id
  left join ss on ss.submarket_id = c.applied_submarket_id
  left join nv on nv.dasher_id = c.dasher_id
)

-- F) Post cohort_asof: count deliveries 7/14/30d — join facts with time predicate (no unbounded CTE)
, post_outcomes as (
  select
    b.dasher_id
  , b.cohort_asof
  , coalesce(
      count_if(
        d.dasher_confirmed_time is not null
        and to_date(d.dasher_confirmed_time) < dateadd('day', 8, b.cohort_asof)
      ), 0
    ) as n_del_0_7d
  , coalesce(
      count_if(
        d.dasher_confirmed_time is not null
        and to_date(d.dasher_confirmed_time) < dateadd('day', 15, b.cohort_asof)
      ), 0
    ) as n_del_0_14d
  , coalesce(
      count_if(
        d.dasher_confirmed_time is not null
        and to_date(d.dasher_confirmed_time) < dateadd('day', 31, b.cohort_asof)
      ), 0
    ) as n_del_0_30d
  from base b
  left join edw.finance.dimension_deliveries d
    on d.dasher_id = b.dasher_id
   and d.dasher_confirmed_time is not null
   and coalesce(d.is_test, false) = false
   and coalesce(d.is_consumer_pickup, false) = false
   and coalesce(d.country_id, 1) = 1
   and d.dasher_confirmed_time::date >= b.cohort_asof
   and d.dasher_confirmed_time::date <  dateadd('day', 31, b.cohort_asof)
  group by b.dasher_id, b.cohort_asof
)

, post_hours as (
  select
    b.dasher_id
  , b.cohort_asof
  , sum(coalesce(dds.adj_shift_seconds, 0)) / nullif(3600.0, 0) as post_cohort_window_shift_hrs_0_30d
  , sum(coalesce(dds.adj_shift_seconds, 0)) as post_cohort_window_shift_sec_0_30d
  from base b
  join edw.dasher.dasher_shifts dds
    on dds.dasher_id = b.dasher_id
  where date(dds.active_date) >= b.cohort_asof
    and date(dds.active_date) <  dateadd('day', 31, b.cohort_asof)
  group by 1, 2
)

-- H) Pre-churn: 28d to last delivery
, pre_churn_shifts as (
  select
    b.dasher_id
  , sum(coalesce(dds.adj_shift_seconds, 0)) / nullif(3600.0, 0) as pre_churn_shift_hrs_28d
  , sum(coalesce(dds.adj_shift_seconds, 0)) / nullif(3600.0 * 4, 0) as pre_churn_avg_weekly_shift_hrs
  , div0(
      sum(dds.total_active_time_seconds),
      nullif(sum(dds.adj_shift_seconds), 0)
    ) as pre_churn_utilization
  , coalesce(
      count_if( dds.adj_shift_seconds is not null and dds.adj_shift_seconds > 0 ),
      0
    ) as pre_churn_n_shift_stubs_28d
  from base b
  join edw.dasher.dasher_shifts dds
    on dds.dasher_id = b.dasher_id
  where dds.active_date::date
    between dateadd('day', -28, b.last_delivery_date) and b.last_delivery_date
  group by 1
)

-- PERF: comment out the entire pre_churn_pre_period CTE and its join in `enriched` if you do not need MoM hours slope
, pre_churn_pre_period as (
  select
    b.dasher_id
  , sum(coalesce(dds.adj_shift_seconds, 0)) / nullif(3600.0, 0) as pre_churn_shift_hrs_prior_28d
  from base b
  join edw.dasher.dasher_shifts dds
    on dds.dasher_id = b.dasher_id
  where dds.active_date::date
    between dateadd('day', -56, b.last_delivery_date) and dateadd('day', -29, b.last_delivery_date)
  group by 1
)

, pre_churn_earnings as (
  select
    b.dasher_id
  , cast(null as number(18,4)) as pre_churn_gross_earnings_28d
  , cast(null as number(18,4)) as pre_churn_earnings_per_hr_28d
  , cast(null as number(18,4)) as pre_churn_tip_per_del_28d
  from base b
  group by 1
)

-- PERF: comment out pre_churn_dash_now and its join in `enriched` (large access-correctness fact) if you only need core shifts/pay
, pre_churn_dash_now as (
  select
    b.dasher_id
  , count(a.active_date) as pre_churn_dn_n_impressions
  , coalesce(sum(iff(coalesce(a.is_dash_now, 0) = 1, 1, 0)), 0) as pre_churn_dn_n_dash_now
  , div0(
      coalesce(sum(iff(coalesce(a.is_dash_now, 0) = 1, 1, 0)), 0),
      nullif(count(a.active_date), 0)
    ) as pre_churn_pct_dash_now
  , case
      when count(a.active_date) = 0 then null
      when div0(
        coalesce(sum(iff(coalesce(a.is_dash_now, 0) = 1, 1, 0)), 0),
        nullif(count(a.active_date), 0)
      ) = 0 then 1
      else 0
    end as pre_churn_dash_now_fully_blocked
  , case
      when count(a.active_date) = 0 then null
      when div0(
        coalesce(sum(iff(coalesce(a.is_dash_now, 0) = 1, 1, 0)), 0),
        nullif(count(a.active_date), 0)
      ) >= 0.5 then 1
      else 0
    end as pre_churn_dash_now_gte_50pct
  from base b
  left join proddb.public.fact_dasher_access_correctness a
    on a.dasher_id = b.dasher_id
   and a.active_date::date between dateadd('day', -28, b.last_delivery_date) and b.last_delivery_date
  group by 1
)

, enriched as (
  select
    b.*
  , p.n_del_0_7d, p.n_del_0_14d, p.n_del_0_30d
  , case when p.n_del_0_7d  > 0 then 1 else 0 end as resurrected_0_7d
  , case when p.n_del_0_14d > 0 then 1 else 0 end as resurrected_0_14d
  , case when p.n_del_0_30d > 0 then 1 else 0 end as resurrected_0_30d
  , coalesce(ph.post_cohort_window_shift_hrs_0_30d, 0) as post_cohort_window_shift_hrs_0_30d
  , pre.pre_churn_shift_hrs_28d
  , pre.pre_churn_avg_weekly_shift_hrs
  , pre.pre_churn_utilization
  , pre.pre_churn_n_shift_stubs_28d
  , ppre.pre_churn_shift_hrs_prior_28d
  , case
      when coalesce(ppre.pre_churn_shift_hrs_prior_28d, 0) > 0
      then (pre.pre_churn_shift_hrs_28d - ppre.pre_churn_shift_hrs_prior_28d)
        / ppre.pre_churn_shift_hrs_prior_28d
      else null
    end as pre_churn_hours_trend_28d_vs_prior_28d
  , pe.pre_churn_gross_earnings_28d
  , pe.pre_churn_earnings_per_hr_28d
  , pe.pre_churn_tip_per_del_28d
  , dn.pre_churn_dn_n_impressions
  , dn.pre_churn_dn_n_dash_now
  , dn.pre_churn_pct_dash_now
  , dn.pre_churn_dash_now_fully_blocked
  , dn.pre_churn_dash_now_gte_50pct
  from base b
  join post_outcomes p
    on p.dasher_id = b.dasher_id
   and p.cohort_asof = b.cohort_asof
  left join post_hours ph
    on ph.dasher_id = b.dasher_id
   and ph.cohort_asof = b.cohort_asof
  left join pre_churn_shifts pre
    on pre.dasher_id = b.dasher_id
  left join pre_churn_pre_period ppre
    on ppre.dasher_id = b.dasher_id
  left join pre_churn_earnings pe
    on pe.dasher_id = b.dasher_id
  left join pre_churn_dash_now dn
    on dn.dasher_id = b.dasher_id
)

, report_resurrection_by_ltd as (
  select
    ltd_bucket
  , count(*) as n_cohort
  , sum(resurrected_0_7d)  as n_res_7d
  , sum(resurrected_0_14d) as n_res_14d
  , sum(resurrected_0_30d) as n_res_30d
  , div0(sum(resurrected_0_7d),  count(*))  as pct_res_7d
  , div0(sum(resurrected_0_14d), count(*)) as pct_res_14d
  , div0(sum(resurrected_0_30d), count(*)) as pct_res_30d
  , div0(sum(coalesce(n_del_0_30d, 0)), count(*)) as avg_del_0_30d
  from enriched
  group by 1
)
, report_resurrected_vs_not as (
  select
    'res_0_30d = 1' as group_label
  , count(*) as n
  , median(pre_churn_avg_weekly_shift_hrs)  as m_pre_churn_wkly_hr
  , median(pre_churn_utilization)           as m_pre_churn_util
  , median(days_since_fd)                    as m_tenure_days
  from enriched
  where resurrected_0_30d = 1
  union all
  select
    'res_0_30d = 0' as group_label
  , count(*)
  , median(pre_churn_avg_weekly_shift_hrs)
  , median(pre_churn_utilization)
  , median(days_since_fd)
  from enriched
  where resurrected_0_30d = 0
)

-- Default: resurrection rates by LTD. Swap final SELECT to report_resurrected_vs_not or add your own.
select * from report_resurrection_by_ltd
order by ltd_bucket
;

-- select * from report_resurrected_vs_not;
-- select * from enriched;
--
-- Example: one row per ltd_bucket with cohort-wide mean pre-churn metrics (replace final select above)
-- select
--   ltd_bucket, count(*) as n_cohort
-- , sum(resurrected_0_30d) as n_res_30d
-- , div0(sum(resurrected_0_30d), count(*)) as pct_res_30d
-- , avg(nullif(pre_churn_shift_hrs_28d, 0)) as avg_pre_churn_shift_hrs_28d
-- , avg(nullif(pre_churn_utilization, 0)) as avg_pre_churn_utilization
-- from enriched
-- group by ltd_bucket
-- order by ltd_bucket;