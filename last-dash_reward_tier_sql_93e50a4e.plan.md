---
name: Last-dash reward tier SQL
overview: Extend your query by attaching each Dasher’s reward tier from `FACT_DASHER_REWARD_TIER` at the timestamp of their latest delivery (same grain as your `latest_delivery_sp` CTE), and propagate that column through the `UNION ALL` so schemas stay aligned.
todos:
  - id: enrich-cte
    content: Add dasher_confirmed_time + LEFT JOIN FACT_DASHER_REWARD_TIER to latest_delivery_sp (or split last_dash_with_tier CTE)
    status: pending
  - id: select-tier
    content: Expose reward_tier_at_last_dash in main SELECT with agg
    status: pending
  - id: union-ads
    content: Join Ads dxlist to same last-dash tier CTE and align SELECT list with first branch
    status: pending
isProject: false
---

# Add reward tier at last dash to your query

## Source of truth for tier

Use **`FACT_DASHER_REWARD_TIER`**, which stores interval history per Dasher. Typical columns: `dasher_id`, **`tier`** (values like `platinum`, `gold`, `silver`, `non_tier`, `new_dasher` per existing internal SQL), `start_time`, `end_time`.

Your workspace already uses two bindings—pick the one your environment expects (they are the same logical table in different databases):

- [`EDW.DASHER.FACT_DASHER_REWARD_TIER`](Cursor Projects/cursor-analytics/team_analytics/personal/zack.mandell@doordash.com/historical_sql/mode/nyc/NYC_Agg_Pay__Bonus_Logic_SoT/zz__SOT_Dx_reward_tier.sql) — `SELECT *` for exploration
- Join pattern at **delivery time** (same idea as “last dash”):

```32:32:Cursor Projects/cursor-analytics/team_analytics/personal/zack.mandell@doordash.com/historical_sql/mode/nyc/2026_03_03_-_NYC_Rewards_Dx_Sentiment/dsat_by_tier.sql
left join EDW.DASHER.FACT_DASHER_REWARD_TIER frt on frt.dasher_id=ds.DASHER_ID and ds.check_out_time between frt.start_time and frt.end_time 
```

For **your** query, replace `check_out_time` with the timestamp that defines the same “last dash” row you already pick in `latest_delivery_sp` (today you order by `dd.dasher_confirmed_time DESC` — use **`dd.dasher_confirmed_time`** as the tier anchor unless product/analytics wants a different event time, e.g. drop-off).

## Implementation approach

### 1. Enrich `latest_delivery_sp` with the tier anchor + tier

Inside the `latest_delivery_sp` CTE (still `FROM dimension_deliveries dd` with the same filters and `QUALIFY ROW_NUMBER() ... = 1`):

- Add **`dd.dasher_confirmed_time`** (or your chosen anchor) to the `SELECT` list.
- **`LEFT JOIN`** `FACT_DASHER_REWARD_TIER` `frt` on:
  - `frt.dasher_id = dd.dasher_id`
  - **Time containment** for that delivery instant. Two equivalent styles used in-repo:
    - **Inclusive window (matches `dsat_by_tier`):**  
      `dd.dasher_confirmed_time between frt.start_time and frt.end_time`
    - **If `end_time` can be NULL** for open-ended current rows, prefer the pattern from [`07_weekly_payout_tracker.sql`](Cursor Projects/cursor-analytics/team_analytics/personal/stuti.madaan@doordash.com/golden_market_committed_sprint/monitoring/sql/07_weekly_payout_tracker.sql):  
      `dd.dasher_confirmed_time >= frt.start_time`  
      `and dd.dasher_confirmed_time < coalesce(frt.end_time, '9999-12-31'::timestamp_tz)`  
      (adjust cast to match column types in Snowflake.)

- Select **`frt.tier as reward_tier_at_last_dash`** (or similar name).

If a Dasher has **no** matching interval (data gap), `LEFT JOIN` yields **NULL** — decide whether to leave null, map to `'unknown'`, or investigate bad joins.

If **multiple** FRT rows could match (unexpected), add **`QUALIFY ROW_NUMBER() OVER (PARTITION BY dd.dasher_id ORDER BY frt.start_time DESC) = 1`** on the join result *before* the outer `QUALIFY` that picks the latest delivery, or collapse in a subquery—only if you observe duplicates.

### 2. Surface the column in the main branch

Change:

`SELECT agg.*`

to explicitly include the new field from the join alias, e.g.:

`SELECT agg.*, l.reward_tier_at_last_dash`

(or list columns if you want a stable column order).

### 3. Keep `UNION ALL` schemas consistent (Ads branch)

Your second branch is:

`select *, 'Ads' as grouping from static.q2_res_ads_dxlist`

After the first branch gains **`reward_tier_at_last_dash`**, the Ads branch must expose the **same** column(s) in the **same order** (Snowflake `UNION ALL` aligns by position).

**Recommended pattern:** define a reusable CTE, e.g. `last_dash_with_tier`, containing at least `dasher_id`, submarket fields used for filters, `dasher_confirmed_time`, and `reward_tier_at_last_dash`. Then:

- **Agg side:** `FROM agg JOIN last_dash_with_tier l ON l.dasher_id = agg.dasher_id` + same `WHERE` on submarkets.
- **Ads side:** `FROM static.q2_res_ads_dxlist ads JOIN last_dash_with_tier l ON l.dasher_id = ads.dasher_id` + **explicit** `SELECT` listing `ads` columns, **`reward_tier_at_last_dash`**, then `'Ads' AS grouping` so column order matches the first `SELECT`.

Apply the **same geography exclusion** on the Ads branch if those Dasher rows should be filtered identically; today the Ads arm does not use `latest_delivery_sp`, so behavior may differ unless you add the same `WHERE`.

## Quick validation queries (run in Snowflake)

1. **Inspect FRT columns:**  
   `select * from edw.dasher.fact_dasher_reward_tier limit 1;`

2. **Spot-check one Dasher:** compare `tier` from the join at `dasher_confirmed_time` vs raw `dimension_deliveries` row for that delivery.

## Summary

| Piece | Action |
|-------|--------|
| Tier table | `FACT_DASHER_REWARD_TIER` (`tier`, `start_time`, `end_time`) |
| Anchor time | Same timestamp as your “last dash” row (likely `dasher_confirmed_time` from `dimension_deliveries`) |
| Join | `dasher_id` + interval containment at that timestamp |
| Union | Add `reward_tier_at_last_dash` to **both** branches with matching column order |

No code edits are applied in this plan-only step; once you confirm, the concrete SQL can be written end-to-end.
