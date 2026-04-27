-- =============================================================================
-- EXTENDED INSIGHTS — lapsed dasher (same `base` / `enriched` as mode_dasher_resurrection_full.sql)
-- =============================================================================
-- No Braze/email. Main file is snapshot-based (`cohort_asof`, lapsed 28+ days, >=10 LTD).
--
-- In mode_dasher_resurrection_full.sql: find the closing `)` of the `enriched` CTE (just
-- before `, report_resurrection_by_ltd`). Add a COMMA and paste **SECTION 1** below
-- in the same `WITH` chain, then add one **SECTION 2** `SELECT` as your final select.
--
-- If FACT_DASHER_REWARD_TIER or peak columns error, `desc` FDDP and fix column names.
-- =============================================================================


-- ##############################################################################
-- SECTION 1 — Paste immediately AFTER `enriched` CTE (after its closing `)`)  as:
--   , pre_churn_pay_28d as ( ...
-- References: base, enriched. Uses last_delivery_date for "pre-lapse" pay window.
-- ##############################################################################

, pre_churn_pay_28d as (
  /* Pre-churn pay: delivery-level, 28d ending on last_delivery_date. USD: *0.01 if cents. */
  select
    b.dasher_id
  , sum(fddp.base_pay_usd) * 0.01   as p28_base_pay_usd
  , sum(fddp.tip_amount_usd) * 0.01  as p28_tip_usd
  , sum(coalesce(fddp.peak_pay_usd, 0)) * 0.01  as p28_peak_pay_usd
  , sum(fddp.actual_pay_usd) * 0.01 as p28_actual_pay_usd
  , count(*)                        as p28_n_pay_rows
  from base b
  join edw.dasher.fact_dasher_delivery_pay fddp
    on fddp.dasher_id = b.dasher_id
  join edw.finance.dimension_deliveries dd
    on dd.delivery_id = fddp.delivery_id
   and dd.shift_id = fddp.shift_id
  where coalesce(fddp.delivery_source, 'MARKETPLACE') not ilike '%DRIVE%'
    and coalesce(dd.is_test, false) = false
    and coalesce(dd.is_consumer_pickup, false) = false
    and coalesce(dd.country_id, 1) = 1
    and dd.dasher_confirmed_time is not null
    and dd.dasher_confirmed_time::date
        between dateadd('day', -28, b.last_delivery_date) and b.last_delivery_date
  group by 1
)
, pre_churn_pay_first14 as (
  select
    b.dasher_id
  , sum(fddp.actual_pay_usd) * 0.01 as p_first14_actual_usd
  , count(*)                         as p_first14_n_dels
  from base b
  join edw.dasher.fact_dasher_delivery_pay fddp
    on fddp.dasher_id = b.dasher_id
  join edw.finance.dimension_deliveries dd
    on dd.delivery_id = fddp.delivery_id
   and dd.shift_id = fddp.shift_id
  where coalesce(fddp.delivery_source, 'MARKETPLACE') not ilike '%DRIVE%'
    and coalesce(dd.is_test, false) = false
    and coalesce(dd.is_consumer_pickup, false) = false
    and dd.dasher_confirmed_time is not null
    and dd.dasher_confirmed_time::date
        between dateadd('day', -28, b.last_delivery_date)
        and dateadd('day', -15, b.last_delivery_date)
  group by 1
)
, pre_churn_pay_last14 as (
  select
    b.dasher_id
  , sum(fddp.actual_pay_usd) * 0.01 as p_last14_actual_usd
  , count(*)                        as p_last14_n_dels
  from base b
  join edw.dasher.fact_dasher_delivery_pay fddp
    on fddp.dasher_id = b.dasher_id
  join edw.finance.dimension_deliveries dd
    on dd.delivery_id = fddp.delivery_id
   and dd.shift_id = fddp.shift_id
  where coalesce(fddp.delivery_source, 'MARKETPLACE') not ilike '%DRIVE%'
    and coalesce(dd.is_test, false) = false
    and coalesce(dd.is_consumer_pickup, false) = false
    and dd.dasher_confirmed_time is not null
    and dd.dasher_confirmed_time::date
        between dateadd('day', -14, b.last_delivery_date) and b.last_delivery_date
  group by 1
)
, frt_tier_at_churn as (
  select
    b.dasher_id
  , frt.tier as reward_tier_at_churn
  from base b
  left join edw.dasher.fact_dasher_reward_tier frt
    on frt.dasher_id = b.dasher_id
   and b.last_delivery_at >= frt.start_time
   and b.last_delivery_at < coalesce(frt.end_time, to_timestamp_ltz('9999-12-31 00:00:00'))
  qualify row_number() over (partition by b.dasher_id order by frt.start_time desc) = 1
)
, enriched_aug as (
  select
    e.*
  , f.reward_tier_at_churn
  , p.p28_base_pay_usd, p.p28_tip_usd, p.p28_peak_pay_usd, p.p28_actual_pay_usd, p.p28_n_pay_rows
  , div0(p.p28_actual_pay_usd, nullif(p.p28_n_pay_rows, 0)) as p28_actual_per_pay_row
  , div0(p.p28_base_pay_usd, nullif(p.p28_n_pay_rows, 0))     as p28_base_per_row
  , div0(p.p28_tip_usd, nullif(p.p28_n_pay_rows, 0))        as p28_tip_per_row
  , div0(p.p28_peak_pay_usd, nullif(p.p28_n_pay_rows, 0))   as p28_peak_per_row
  , f14.p_first14_actual_usd, f14.p_first14_n_dels
  , l14.p_last14_actual_usd,  l14.p_last14_n_dels
  , case
      when coalesce(f14.p_first14_actual_usd, 0) > 0
      then (l14.p_last14_actual_usd - f14.p_first14_actual_usd) / f14.p_first14_actual_usd
      else null
    end as p28_actual_pay_trend_first_vs_last_14d
  from enriched e
  left join pre_churn_pay_28d p
    on p.dasher_id = e.dasher_id
  left join pre_churn_pay_first14 f14
    on f14.dasher_id = e.dasher_id
  left join pre_churn_pay_last14 l14
    on l14.dasher_id = e.dasher_id
  left join frt_tier_at_churn f
    on f.dasher_id = e.dasher_id
)


-- ##############################################################################
-- SECTION 2 — One final SELECT (replace the default in main file or add here).
--    Use `enriched_aug` where pay / reward_tier are needed. Remove report_* CTEs from
--    the main file if you only need these, or add `,` after enriched_aug and reference it.
-- ##############################################################################

-- 2A) Resurrection in 30d from cohort_asof, by reward tier (at last dash) & product tier & LTD
-- select
--   coalesce(reward_tier_at_churn, 'unknown') as reward_tier_at_churn
-- , coalesce(tier, 'unknown')                 as product_tier_snapshot
-- , ltd_bucket
-- , count(*) as n
-- , sum(resurrected_0_30d)                 as n_res_30d
-- , div0(sum(resurrected_0_30d), count(*))  as pct_res_30d
-- from enriched_aug
-- group by 1, 2, 3
-- order by pct_res_30d desc, n desc;

-- 2B) Resurrected vs not (30d) — pre-lapse pay & utilization (medians)
-- select
--   case when resurrected_0_30d = 1 then 'resurrected' else 'not_resurrected' end as group_label
-- , count(*) as n
-- , median(p28_actual_per_pay_row)   as m_p28_actual_per_del
-- , median(p28_base_per_row)         as m_p28_base_per_row
-- , median(p28_tip_per_row)         as m_p28_tip_per_row
-- , median(p28_peak_per_row)         as m_p28_peak_per_row
-- , median(p28_actual_pay_trend_first_vs_last_14d) as m_trend_14d_split
-- , median(pre_churn_avg_weekly_shift_hrs)  as m_prechurn_wkly_hr
-- , median(pre_churn_utilization)            as m_prechurn_util
-- , median(pre_churn_pct_dash_now)           as m_prechurn_pct_dash_now
-- , sum(iff(pre_churn_dash_now_fully_blocked = 1, 1, 0)) as n_dn_fully_blocked
-- from enriched_aug
-- group by 1
-- order by 1;

-- 2C) Same, by LTD bucket
-- select
--   ltd_bucket
-- , case when resurrected_0_30d = 1 then 'resurrected' else 'not_resurrected' end as group_label
-- , count(*) as n
-- , div0(sum(resurrected_0_30d), count(*)) as pct_res_30d
-- , median(p28_actual_per_pay_row) as m_actual_per_del
-- , median(pre_churn_utilization)  as m_util
-- from enriched_aug
-- group by 1, 2
-- order by 1, 2;

-- 2D) Delivery count 0-30d after cohort_asof among resurrected, by LTD
-- select
--   ltd_bucket
-- , count(*) as n_resurrected
-- , div0(sum(n_del_0_30d), count(*)) as avg_dels_0_30d_per_resurrected
-- from enriched_aug
-- where resurrected_0_30d = 1
-- group by 1
-- order by 1;

-- 2E) Crimson + product flags
-- select
--   has_crimson, crimson_durable_user, has_done_dsd, has_done_alcohol
-- , count(*) as n
-- , div0(sum(resurrected_0_30d), count(*)) as pct_res_30d
-- from enriched_aug
-- group by 1, 2, 3, 4
-- order by n desc;


-- ##############################################################################
-- SECTION 3 — <10 vs >=10 LTD: relax `and dx.lifetime_num_deliveries_made >= 10` in main `base`.
-- ##############################################################################

-- ##############################################################################
-- SECTION 4 — Optional acceptance (heavy; validate join path in your env)
-- , pre_churn_assign_stats as (
--   select b.dasher_id
--   , div0(
--       count_if(sda.accepted_at is not null and sda.unassigned_at is null),
--       nullif(count(sda.delivery_package_id), 0)
--     ) as acceptance_rate_prechurn_28d
--   from base b
--   left join proddb.prod_assignment.shift_delivery_assignment sda
--     on sda.dasher_id = b.dasher_id
--    and sda.created_at::date between dateadd('day', -28, b.last_delivery_date) and b.last_delivery_date
--   group by 1
-- )
-- ##############################################################################
