---
name: Dasher fleet churn SQL
overview: Define churn as “active in baseline period, inactive in the next period,” then compute churn % = churned / baseline-active dashers. Below is Snowflake-style SQL using delivery activity; you can swap the activity source or grain (week vs month) to match your org’s definition.
todos:
  - id: pick-grain
    content: Choose period grain (month vs week) and baseline/churn date range
    status: pending
  - id: pick-activity
    content: Choose single activity source (deliveries vs shifts) and filters (country, is_filtered, etc.)
    status: pending
  - id: implement-cte
    content: Build monthly_active → baseline_fleet → churned → div0 churn_pct
    status: pending
  - id: validate
    content: Reconcile fleet size and spot-check churned dashers for zero activity in churn period
    status: pending
isProject: false
---

# Dasher fleet churn % (SQL pattern)

## Definitions (defaults you can change)

- **Baseline “fleet” (denominator):** dashers with at least one qualifying delivery in **period A** (e.g. calendar month *M*−1).
- **Churned:** dashers in that fleet with **zero** qualifying deliveries in **period B** (e.g. calendar month *M*).
- **Churn %:** `churned_dashers / fleet_dashers`.

This is the usual **month-over-month churn among last month’s active dashers**. If you instead want “% of all-time dashers who are currently inactive N days,” that is a different numerator/denominator—swap the CTEs accordingly.

## Core logic (deliveries-based)

Use one row per dasher per calendar month of activity, then compare two months.

```sql
-- Parameters: set analysis months (or switch to week buckets)
with params as (
  select
    date '2026-03-01' as baseline_month_start,
    date '2026-04-01' as churn_month_start
),

-- Example: qualifying deliveries (adjust filters to your “active” definition)
dd_filtered as (
  select
    dd.dasher_id,
    date_trunc('month', dd.active_date)::date as activity_month
  from dimension_deliveries dd
  join params p on true
  where dd.country_id = 1
    and dd.is_filtered = true
    and dd.active_date >= p.baseline_month_start
    and dd.active_date <  dateadd('month', 2, p.baseline_month_start) -- covers baseline + churn month
),

monthly_active as (
  select distinct
    dasher_id,
    activity_month
  from dd_filtered
),

baseline_fleet as (
  select distinct m.dasher_id
  from monthly_active m
  join params p on m.activity_month = p.baseline_month_start
),

churned as (
  select b.dasher_id
  from baseline_fleet b
  left join monthly_active m
    on m.dasher_id = b.dasher_id
   and m.activity_month = (select churn_month_start from params)
  where m.dasher_id is null
)

select
  (select count(*) from baseline_fleet) as fleet_dashers,
  (select count(*) from churned)        as churned_dashers,
  div0((select count(*) from churned), (select count(*) from baseline_fleet)) as churn_pct;
```

## Why this beats joining raw `dimension_deliveries` in the final step

- **Distinct months first** avoids row explosion from many deliveries per dasher.
- **Clear period alignment:** baseline and churn windows are explicit.

## Common variants (same structure)

| Goal | Change |
|------|--------|
| **Week-over-week** | Replace `date_trunc('month', …)` with `date_trunc('week', …)` and set two week start dates in `params`. |
| **Activity from shifts** | Build `monthly_active` from `edw.dasher.dasher_shifts` (e.g. `check_in_time::date`) with your shift filters instead of `dimension_deliveries`. |
| **Geography** | Join dasher’s submarket (from your latest-delivery or home submarket logic) and filter `submarket_id` / region. |
| **“Fleet” = active in last 90 days** | Denominator = distinct dashers with activity in rolling 90 days ending *today*; churned = those with no activity in the subsequent 30 days—different `params` and rolling windows. |

## Pitfalls to avoid

- **Inconsistent activity rules:** If baseline uses deliveries but “inactive” uses shifts (or vice versa), the % will drift. Pick one source for both periods.
- **`left join dimension_deliveries` + `where active_date between …`:** That pattern filters on the right table and acts like an existence filter; for churn, prefer **pre-aggregating** activity by dasher and period (as above).
- **Duplicate dashers in static lists:** If you merge cohort tables with `union all` without deduping, denominators can double-count—use `union` or `select distinct dasher_id` for fleet counts.

## Optional sanity checks

- Reconcile **fleet** count to a simple `count(distinct dasher_id)` for the baseline month from the same `dd_filtered` rules.
- Spot-check a few `churned` dashers to confirm zero activity in the churn month under the same filters.

No repository files are required for this; implement directly in Snowflake with your chosen dates and filters.
