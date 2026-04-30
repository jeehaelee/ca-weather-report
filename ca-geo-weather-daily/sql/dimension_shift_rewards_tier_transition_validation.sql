-- =============================================================================
-- Validate whether dimension_shift_rewards (+ shift_starting_tier) supports
-- pre/post tier *transition* analysis (consecutive-shift upgrade/downgrade/same).
--
-- HOW TO USE
-- 1) fqtn is set to proddb.public.dimension_shift_rewards (discovered in information_schema).
-- 2) Fix column names in the CTE `s` if they differ (run SECTION 0 first).
-- 3) Run each section; pass/fail notes are in comments.
--
-- RUNNING QUERIES (local / agent on your machine)
-- Preferred (no Okta each time): key-pair JWT — see
--   ca-geo-weather-daily/docs/snowflake_keypair_auth.md
-- Then:
--   ./ca-geo-weather-daily/scripts/run_snow_query.sh -f ca-geo-weather-daily/sql/dimension_shift_rewards_tier_transition_validation.sql
-- (script defaults to connection DOORDASH-DOORDASH-JWT; use SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH for SSO.)
--
-- SSO only: in a terminal with a browser, `snow connection test` when the cache expires;
-- client_store_temporary_credential on DOORDASH-DOORDASH reduces but does not eliminate prompts.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SECTION 0 — Resolve real column names (run once; paste results into `s` below)
-- -----------------------------------------------------------------------------
-- select table_catalog, table_schema, table_name, column_name, data_type
-- from information_schema.columns
-- where table_name ilike 'dimension_shift_rewards'
-- order by table_catalog, table_schema, ordinal_position;

-- -----------------------------------------------------------------------------
-- CONFIG — replace with your warehouse object + actual column identifiers
-- -----------------------------------------------------------------------------
-- Resolved via information_schema (PRODDB, name ilike '%shift%reward%'):
--   proddb.public.dimension_shift_rewards
set fqtn = 'proddb.public.dimension_shift_rewards';

-- -----------------------------------------------------------------------------
-- SECTION 1 — Grain: is the table one row per shift?
-- Pass: row_count = distinct_shift_id (or explainable ratio if snapshots/versioned)
-- -----------------------------------------------------------------------------
/*
select
    count(*) as row_count,
    count(distinct shift_id) as distinct_shift_id,
    count(*) - count(distinct shift_id) as duplicate_shift_id_rows
from identifier($fqtn);
*/

-- -----------------------------------------------------------------------------
-- SECTION 2 — Required fields populated for tier sequencing
-- Pass: null_dasher / null_shift_start / null_starting_tier near 0
-- -----------------------------------------------------------------------------
/*
select
    count(*) as rows_total,
    sum(iff(dasher_id is null, 1, 0)) as null_dasher_id,
    sum(iff(shift_start_ts is null, 1, 0)) as null_shift_start,
    sum(iff(shift_starting_tier is null, 1, 0)) as null_shift_starting_tier
from (
    select
        dasher_id,
        shift_start_ts,
        shift_starting_tier
    from identifier($fqtn)
    where shift_start_ts::date between '2026-01-09' and '2026-04-28'
) x;
*/

-- -----------------------------------------------------------------------------
-- SECTION 3 — Tier domain looks like the product taxonomy
-- Pass: only expected tier labels (plus rare unknowns to investigate)
-- -----------------------------------------------------------------------------
/*
select
    shift_starting_tier,
    count(*) as n_shifts
from identifier($fqtn)
where shift_start_ts::date between '2026-01-09' and '2026-04-28'
group by 1
order by n_shifts desc;
*/

-- -----------------------------------------------------------------------------
-- SECTION 4 — Dedup risk on (dasher_id, shift_id) or (dasher_id, shift_start_ts)
-- Pass: dup_pairs = 0 for the key you will use to order shifts
-- -----------------------------------------------------------------------------
/*
select
    count(*) as dup_pairs
from (
    select dasher_id, shift_id, count(*) as c
    from identifier($fqtn)
    where shift_start_ts::date between '2026-01-09' and '2026-04-28'
    group by 1, 2
    having count(*) > 1
) d;
*/

-- -----------------------------------------------------------------------------
-- SECTION 5 — Ordering sanity: multiple shifts per dasher in window
-- Pass: high pct of dashers with 2+ shifts if you want stable consecutive transitions
-- -----------------------------------------------------------------------------
/*
with s as (
    select
        dasher_id,
        shift_id,
        shift_start_ts,
        shift_starting_tier
    from identifier($fqtn)
    where shift_start_ts::date between '2026-01-09' and '2026-04-28'
),
per_dx as (
    select dasher_id, count(*) as n_shifts
    from s
    group by 1
)
select
    sum(iff(n_shifts >= 2, 1, 0)) as dx_with_2plus_shifts,
    count(*) as dx_total,
    dx_with_2plus_shifts / nullif(dx_total, 0) as pct_dx_with_2plus_shifts
from per_dx;
*/

-- -----------------------------------------------------------------------------
-- SECTION 6 — Consecutive-shift tier change (core capability test)
-- Requires: tier rank mapping consistent with product order (edit mapping CTE).
-- Pass: sensible distribution of up/same/down; no explosion of null "rank"
-- -----------------------------------------------------------------------------
/*
with tier_rank as (
    select column1 as tier, column2 as rnk from values
        ('new_dasher', 0),
        ('non_tier', 1),
        ('silver', 2),
        ('gold', 3),
        ('platinum', 4)
),
s as (
    select
        dasher_id,
        shift_id,
        shift_start_ts,
        lower(nullif(trim(shift_starting_tier), '')) as tier
    from identifier($fqtn)
    where shift_start_ts::date between '2026-01-09' and '2026-04-28'
),
ordered as (
    select
        s.*,
        tr.rnk as tier_rnk,
        lag(tr.rnk) over (partition by dasher_id order by shift_start_ts, shift_id) as prev_rnk,
        lag(tier) over (partition by dasher_id order by shift_start_ts, shift_id) as prev_tier
    from s
    left join tier_rank tr
        on tr.tier = s.tier
),
classified as (
    select
        *,
        case
            when prev_rnk is null then 'first_shift_in_window'
            when tier_rnk is null or prev_rnk is null then 'unmapped_tier'
            when tier_rnk > prev_rnk then 'upgrade'
            when tier_rnk = prev_rnk then 'same'
            when tier_rnk < prev_rnk then 'downgrade'
        end as transition
    from ordered
)
select
    transition,
    count(*) as n_transitions
from classified
where transition <> 'first_shift_in_window'
group by 1
order by n_transitions desc;
*/

-- -----------------------------------------------------------------------------
-- SECTION 7 — Pre vs post *first observed* shift tier in window (not full stock;
-- diagnostic only). Align dates with your experiment readout.
-- -----------------------------------------------------------------------------
/*
with s as (
    select
        dasher_id,
        shift_id,
        shift_start_ts,
        shift_starting_tier,
        case when shift_start_ts::date >= '2026-02-07' then 'post' else 'pre' end as phase
    from identifier($fqtn)
    where shift_start_ts::date between '2026-01-09' and '2026-04-28'
),
first_per_phase as (
    select
        dasher_id,
        phase,
        shift_starting_tier as tier_at_first_shift,
        row_number() over (partition by dasher_id, phase order by shift_start_ts, shift_id) as rn
    from s
)
select
    phase,
    tier_at_first_shift,
    count(*) as n_dashers
from first_per_phase
where rn = 1
group by 1, 2
order by 1, 3 desc;
*/

-- Uncomment one SECTION at a time in Snowflake/Mode (or use run_snow_query.sh -f).
