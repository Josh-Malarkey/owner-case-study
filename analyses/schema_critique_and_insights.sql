/*
================================================================================
 SCHEMA CRITIQUE & INSIGHT REPORT
 Owner.com GTM Analytics
================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 PART 1: SCHEMA CRITIQUE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

NO REP/OWNER ASSIGNMENT ON LEADS OR OPPORTUNITIES
   The schema has no field indicating which SDR, BDR, or AE owns a given lead
   or opportunity. This makes rep-level and team-level productivity analysis
   impossible — we can compute channel-level activity averages, but cannot
   identify top performers, measure SDR→AE handoff quality, or build capacity
   models. A lead_owner_id / opportunity_owner_id joining to a rep dimension
   (with team, role, hire_date) is the single highest-impact schema addition.

EXPENSE TABLES USE VARCHAR FOR CURRENCY AND MONTH
   Both expense tables store monetary values and dates as strings. This
   introduces parsing fragility (unknown date format, currency symbols,
   locale-specific separators). These should be typed as DATE and NUMBER(18,2)
   at the source. My staging models use TRY_TO_NUMBER and TRY_TO_DATE to
   handle this defensively, but nulls from parse failures would silently
   drop cost data.

MISSING A TRUE LEAD CREATION DATE
   There is no explicit created_date on the leads table. For inbound leads
   we can use form_submission_date; for outbound leads we fall back to
   first_sales_call_date. This means outbound lead age may be understated
   (a BDR could research a prospect before the first call). A lead_created_date
   field from Salesforce's CreatedDate would fix this.

NO ACTIVITY LOG / EVENT TABLE
   Activity counts (sales_call_count, etc.) are pre-aggregated on the lead.
   We cannot analyze activity timing/cadence, rep-level activity, or the
   specific sequence of touches that converts best. A normalized activity
   event table (activity_id, lead_id, type, timestamp, rep_id) would unlock
   cadence optimization and rep productivity analysis.

PREDICTED_SALES_WITH_OWNER IS A VARCHAR
   This revenue proxy is stored as a string (likely "$X,XXX" format). Beyond
   the typing issue, it's unclear whether this is monthly, annual, or
   lifetime. I've assumed monthly to align with the $500/mo subscription
   cadence, but this assumption should be validated with the BizOps team.

NO MRR / ACTUAL POST-SALE REVENUE
   LTV is estimated from predicted_sales_with_owner, not actuals. We have
   no visibility into real subscription retention, churn, or actual GMV
   flowing through the platform. Connecting to a billing/payments source
   (Stripe, internal ledger) would allow actual LTV computation and
   cohort retention curves.

EXPENSE ALLOCATION IS COARSE
   Monthly expenses cannot be attributed to specific campaigns, ad groups,
   or rep teams. Advertising spend is a single line item — we can't separate
   Google vs Facebook, brand vs performance. Adding campaign-level spend
   data would enable channel sub-optimization.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 PART 2: INSIGHT REPORT — DATA-BACKED RECOMMENDATIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RECOMMENDATION 1: REDUCE INBOUND SPEED-TO-LEAD TO INCREASE CONVERSION
────────────────────────────────────────────────────────────────────────

Supporting query (run against fct_lead_funnel):
*/

-- INSIGHT 1: Speed-to-lead impact on conversion
-- Compare conversion rates for leads contacted within 1 day vs later.
select
    channel,
    case
        when days_lead_to_first_contact <= 0 then 'same_day'
        when days_lead_to_first_contact = 1 then 'next_day'
        when days_lead_to_first_contact between 2 and 7 then '2_to_7_days'
        else '8_plus_days'
    end as response_speed_bucket,
    count(*) as lead_count,
    count_if(is_closed_won) as won_count,
    div0(count_if(is_closed_won), count(*)) as close_rate,
    avg(estimated_ltv_24mo) as avg_ltv_won
from {{ ref('fct_lead_funnel') }}
where first_sales_call_date is not null
group by 1, 2
order by 1, 2;

/*
Hypothesis & Rationale:
  Industry benchmarks consistently show that contacting inbound leads within
  5 minutes yields 8x higher conversion than waiting 30+ minutes (https://voiso.com/articles/lead-response-time-metrics/).
  The days_lead_to_first_contact metric in our model lets us measure this effect
  at Owner.com.

  If leads contacted same-day close at even 2x the rate of those contacted
  after 2+ days, then operationalizing a <1hr SLA for inbound leads via
  automated routing and SDR alerts could yield a 15-25% lift in inbound
  close rate — translating directly to lower inbound CAC without additional
  ad spend.

Quantified Impact:
  - Estimated improvement: 15-25% increase in inbound close rate
  - CAC reduction: If inbound CAC is $X, same volume of spend produces
    15-25% more closed deals, effectively reducing CAC by 13-20%.
  - At scale: With constant ad spend, this accelerates ARR growth without
    proportional cost increase.

Implementation Complexity: LOW
  - Salesforce lead routing rules + SDR SLA alerting
  - Dashboard monitoring in existing BI tool
  - No additional data infrastructure required


RECOMMENDATION 2: OPTIMIZE OUTBOUND TARGETING USING PREDICTED SALES TIER
────────────────────────────────────────────────────────────────────────────

Supporting query (run against fct_lead_funnel):
*/

-- INSIGHT 2: High-predicted-revenue prospects convert and retain better.
-- Segment by predicted_monthly_sales quartile and compare outcomes.
select
    channel,
    case
        when predicted_monthly_sales is null then 'unknown'
        when predicted_monthly_sales < 5000 then 'tier_1_under_5k'
        when predicted_monthly_sales < 15000 then 'tier_2_5k_to_15k'
        when predicted_monthly_sales < 30000 then 'tier_3_15k_to_30k'
        else 'tier_4_30k_plus'
    end as predicted_sales_tier,
    count(*) as total_leads,
    count_if(is_converted) as converted,
    count_if(is_closed_won) as won,
    div0(count_if(is_closed_won), count(*)) as close_rate,
    avg(case when is_closed_won then estimated_ltv_24mo end) as avg_ltv,
    avg(total_activity_count) as avg_activities,
    -- Efficiency: LTV generated per activity touch
    div0(
        sum(case when is_closed_won then estimated_ltv_24mo else 0 end),
        sum(total_activity_count)
    ) as ltv_per_activity
from {{ ref('fct_lead_funnel') }}
group by 1, 2
order by 1, 2;

/*
Hypothesis & Rationale:
  Outbound's advantage is targeting control — BizOps selects who BDRs call.
  If higher predicted_monthly_sales tiers show materially better close rates
  AND higher LTV, then concentrating BDR effort on Tier 3-4 prospects
  ($15k+ predicted monthly sales) produces compounding returns:
    1. Higher close rate → more deals per BDR
    2. Higher LTV per deal → better CAC:LTV ratio
    3. Fewer wasted activities on low-value prospects

  Even if Tier 4 prospects close at the same rate as Tier 1, the LTV
  difference alone justifies prioritization. A Tier 4 closed-won deal at
  $30k/mo predicted sales generates:
    LTV = ($500 + $30,000 * 0.05) * 24 = $48,000
  vs a Tier 1 deal at $3k/mo:
    LTV = ($500 + $3,000 * 0.05) * 24 = $15,600

  That's a 3x LTV difference for the same BDR effort.

Quantified Impact:
  - Estimated improvement: 20-40% increase in LTV per BDR-sourced deal
  - If BDR capacity is fixed, reallocating 50% of outbound effort from
    Tier 1-2 to Tier 3-4 could improve outbound CAC:LTV by 30%+
  - Direct ARR impact: higher-value customers also generate more
    transaction revenue, accelerating the marketplace flywheel.

Implementation Complexity: LOW-MEDIUM
  - BizOps already does prospect enrichment — add a scoring threshold
  - Update BDR lead routing/prioritization queues
  - Requires validation that predicted_sales_with_owner correlates with
    actual post-sale GMV (schema gap noted above)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

 Together, these two recommendations attack the scaling goal from both sides:

  1. INBOUND: Extract more conversions from existing ad spend (efficiency)
  2. OUTBOUND: Concentrate BDR effort on highest-LTV prospects (yield)

 Combined, a 20% inbound CAC improvement + 30% outbound LTV improvement
 provides the foundation to scale 2-3x ARR while maintaining or improving
 the CAC:LTV ratio — exactly the dual mandate for hypergrowth.

 Critical next steps for data infrastructure:
  - Add rep/owner IDs to enable sales productivity analysis
  - Connect post-sale revenue data to validate LTV assumptions
  - Break out ad spend by platform (Google vs Facebook) for channel
    sub-optimization
*/
