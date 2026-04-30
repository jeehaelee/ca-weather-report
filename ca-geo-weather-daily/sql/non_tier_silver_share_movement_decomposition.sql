-- =============================================================================
-- HOW TO RUN (from repo root, in a real terminal so Okta can open, or use JWT)
--   SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH \
--     ./ca-geo-weather-daily/scripts/run_snow_sql_file.sh \
--     ca-geo-weather-daily/sql/non_tier_silver_share_movement_decomposition.sql
-- Headless: register key per ca-geo-weather-daily/docs/snowflake_keypair_auth.md, then
--   SNOWFLAKE_CONNECTION=DOORDASH-DOORDASH-JWT ./ca-geo-weather-daily/scripts/run_snow_sql_file.sh ...
-- =============================================================================
-- Decompose *drivers* of non_tier + silver share movement (pre vs post, by arm).
--
-- Why aggregate "% DAD by tier" CSVs cannot answer "% of movement due to X":
--   Those files are period-level shares. They mix (a) who stays in a tier,
--   (b) who upgrades/downgrades, (c) who churns, (d) who enters/reactivates
--   with a tier label — all in one number. Attribution needs a *panel* (same
--   dashers over time) or a structural model.
--
-- What this script does instead:
--   Uses shift-grain tier (`shift_starting_tier` on dimension_shift_rewards) to
--   label each dasher's *last tier in pre* and *first tier in post* (among
--   shifters). Classifies flows for anyone with a pre-window shift; optionally
--   flags post-only entrants. Join YOUR experiment roster for T vs C.
--
-- How to read results:
--   - Compare T vs C in P(upgrade_out | was_low_pre), P(churn | was_low_pre),
--     P(stayed_low | was_low_pre), P(downgrade_into_low | was_high_pre), etc.
--   - "Dominant mechanism" for differential *exit from low tier* is usually
--     whichever rate gap (T−C) is largest among those conditioning on low pre.
--   - Exact "% of DiD in headline tier share" needs an accounting / regression
--     bridge; this SQL gives the flow table that feeds that narrative or model.
--
-- Prerequisites: run SECTION 0–2 of dimension_shift_rewards_tier_transition_validation.sql
--   to confirm column names and grain. Adjust fqtn / column identifiers below.
-- =============================================================================

set fqtn = 'proddb.public.dimension_shift_rewards';

-- Pre/post windows (align with your readout).
set pre_start  = '2026-01-09';
set pre_end    = '2026-01-22';
set post_start = '2026-02-07';
set post_end   = '2026-04-28';

-- -----------------------------------------------------------------------------
-- 1) REPLACE: experiment roster with dasher_id + arm (e.g. ca_treatment, control)
-- -----------------------------------------------------------------------------
create or replace temp table tmp_experiment_arm as
select
    cast(null as number) as dasher_id,
    cast(null as varchar) as arm
where 1 = 0;
-- Example — uncomment and point at your assignment table:
-- create or replace temp table tmp_experiment_arm as
-- select distinct
--     dasher_id,
--     lower(trim(experiment_arm)) as arm
-- from your_db.your_schema.ca_rewards_experiment_roster
-- where experiment_arm in ('ca_treatment', 'control');

-- -----------------------------------------------------------------------------
-- 2) Shift-level extract (edit column names if SECTION 0 differs)
-- -----------------------------------------------------------------------------
create or replace temp table tmp_shifts as
select
    dasher_id,
    shift_id,
    shift_start_ts,
    lower(nullif(trim(shift_starting_tier), '')) as tier,
    shift_start_ts::date as d
from identifier($fqtn)
where shift_start_ts::date between to_date($pre_start) and to_date($post_end);

create or replace temp table tmp_last_pre as
select
    dasher_id,
    tier as tier_last_pre
from (
    select
        dasher_id,
        tier,
        row_number() over (
            partition by dasher_id
            order by shift_start_ts desc, shift_id desc
        ) as rn
    from tmp_shifts
    where d between to_date($pre_start) and to_date($pre_end)
)
where rn = 1;

create or replace temp table tmp_first_post as
select
    dasher_id,
    tier as tier_first_post
from (
    select
        dasher_id,
        tier,
        row_number() over (
            partition by dasher_id
            order by shift_start_ts asc, shift_id asc
        ) as rn
    from tmp_shifts
    where d between to_date($post_start) and to_date($post_end)
)
where rn = 1;

create or replace temp table tmp_activity as
select
    dasher_id,
    max(iff(d between to_date($pre_start) and to_date($pre_end), 1, 0))  as had_pre_shift,
    max(iff(d between to_date($post_start) and to_date($post_end), 1, 0)) as had_post_shift
from tmp_shifts
group by 1;

-- low tier = non_tier ∪ silver (headline bucket from your charts)
create or replace temp table tmp_flow as
select
    a.dasher_id,
    a.had_pre_shift,
    a.had_post_shift,
    lp.tier_last_pre,
    fp.tier_first_post,
    iff(lp.tier_last_pre in ('non_tier', 'silver'), 1, 0) as low_pre,
    iff(fp.tier_first_post in ('non_tier', 'silver'), 1, 0) as low_post,
    case
        when a.had_pre_shift = 0 and a.had_post_shift = 1 then 'post_only_entrant'
        when a.had_pre_shift = 1 and a.had_post_shift = 0 then 'churn_after_pre'
        when a.had_pre_shift = 1 and a.had_post_shift = 1
             and lp.tier_last_pre in ('non_tier', 'silver')
             and fp.tier_first_post not in ('non_tier', 'silver') then 'upgrade_out_of_low'
        when a.had_pre_shift = 1 and a.had_post_shift = 1
             and lp.tier_last_pre in ('non_tier', 'silver')
             and fp.tier_first_post in ('non_tier', 'silver') then 'stayed_low'
        when a.had_pre_shift = 1 and a.had_post_shift = 1
             and coalesce(lp.tier_last_pre, '') not in ('non_tier', 'silver')
             and fp.tier_first_post in ('non_tier', 'silver') then 'downgrade_into_low'
        when a.had_pre_shift = 1 and a.had_post_shift = 1
             and coalesce(lp.tier_last_pre, '') not in ('non_tier', 'silver')
             and coalesce(fp.tier_first_post, '') not in ('non_tier', 'silver') then 'stayed_non_low'
        when a.had_pre_shift = 1 and a.had_post_shift = 1 then 'other_transition'
        else 'no_shifts_in_windows'
    end as flow_bucket
from tmp_activity a
left join tmp_last_pre lp
    on lp.dasher_id = a.dasher_id
left join tmp_first_post fp
    on fp.dasher_id = a.dasher_id;

-- -----------------------------------------------------------------------------
-- 3) Arm-level summary (requires non-empty tmp_experiment_arm)
-- -----------------------------------------------------------------------------
select
    e.arm,
    f.flow_bucket,
    count(*) as n_dashers
from tmp_flow f
inner join tmp_experiment_arm e
    on e.dasher_id = f.dasher_id
where e.arm is not null
group by 1, 2
order by 1, 3 desc;

-- -----------------------------------------------------------------------------
-- 4) Rates conditional on *was low tier at last pre shift* (core for silver/non_tier story)
--    Run after roster exists; empty roster returns no rows.
-- -----------------------------------------------------------------------------
select
    e.arm,
    count(*) as n_low_at_last_pre,
    sum(iff(f.flow_bucket = 'upgrade_out_of_low', 1, 0)) as n_upgrade_out,
    sum(iff(f.flow_bucket = 'stayed_low', 1, 0)) as n_stayed_low,
    sum(iff(f.flow_bucket = 'churn_after_pre', 1, 0)) as n_churned,
    sum(iff(f.flow_bucket in ('other_transition', 'post_only_entrant', 'no_shifts_in_windows'), 1, 0)) as n_other,
    div0(sum(iff(f.flow_bucket = 'upgrade_out_of_low', 1, 0)), count(*)) as rate_upgrade_out,
    div0(sum(iff(f.flow_bucket = 'stayed_low', 1, 0)), count(*)) as rate_stayed_low,
    div0(sum(iff(f.flow_bucket = 'churn_after_pre', 1, 0)), count(*)) as rate_churned
from tmp_flow f
inner join tmp_experiment_arm e
    on e.dasher_id = f.dasher_id
where e.arm is not null
  and f.had_pre_shift = 1
  and f.low_pre = 1
group by 1
order by 1;

-- -----------------------------------------------------------------------------
-- 5) No experiment roster yet: global counts for dashers *low at last pre shift*
-- -----------------------------------------------------------------------------
select
    flow_bucket,
    count(*) as n_dashers,
    div0(count(*), sum(count(*)) over ()) as pct_within_low_pre_cohort
from tmp_flow
where had_pre_shift = 1
  and low_pre = 1
group by 1
order by 2 desc;
