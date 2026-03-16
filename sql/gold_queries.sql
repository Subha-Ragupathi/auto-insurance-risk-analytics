-- =============================================================================
-- Gold Layer — Analytical SQL Queries
-- Target: Databricks SQL Warehouse / Notebook %sql cells
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Overall portfolio claim rate
-- -----------------------------------------------------------------------------
SELECT
    COUNT(policy_id)                          AS total_policies,
    SUM(claim_status)                         AS total_claims,
    ROUND(AVG(claim_status), 4)               AS overall_claim_rate,
    ROUND(AVG(subscription_length), 2)        AS avg_subscription_months
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`;


-- -----------------------------------------------------------------------------
-- 2. Top 10 regions by claim rate (minimum 100 policies for credibility)
-- -----------------------------------------------------------------------------
SELECT
    region_code,
    total_policies,
    total_claims,
    claim_rate,
    avg_subscription_length
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_region`
WHERE total_policies >= 100
ORDER BY claim_rate DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 3. Claim rate by fuel type — ranked
-- -----------------------------------------------------------------------------
SELECT
    fuel_type,
    total_policies,
    total_claims,
    claim_rate,
    RANK() OVER (ORDER BY claim_rate DESC) AS risk_rank
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_fuel_type`;


-- -----------------------------------------------------------------------------
-- 4. Claim rate by customer age band — with portfolio share
-- -----------------------------------------------------------------------------
SELECT
    customer_age_band,
    total_policies,
    total_claims,
    claim_rate,
    ROUND(total_policies * 100.0 / SUM(total_policies) OVER (), 2) AS pct_of_portfolio
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_customer_age_band`
ORDER BY claim_rate DESC;


-- -----------------------------------------------------------------------------
-- 5. Claim rate by vehicle age band
-- -----------------------------------------------------------------------------
SELECT
    vehicle_age_band,
    total_policies,
    total_claims,
    claim_rate,
    ROUND(total_policies * 100.0 / SUM(total_policies) OVER (), 2) AS pct_of_portfolio
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_vehicle_age_band`
ORDER BY claim_rate DESC;


-- -----------------------------------------------------------------------------
-- 6. Claim rate by segment
-- -----------------------------------------------------------------------------
SELECT
    segment,
    total_policies,
    total_claims,
    claim_rate
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_segment`
ORDER BY claim_rate DESC;


-- -----------------------------------------------------------------------------
-- 7. NCAP safety rating vs claim rate — does safer mean fewer claims?
-- -----------------------------------------------------------------------------
SELECT
    ncap_rating,
    total_policies,
    total_claims,
    claim_rate,
    avg_customer_age
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_ncap_rating`
ORDER BY ncap_rating;


-- -----------------------------------------------------------------------------
-- 8. High-risk segments: cross-join of fuel + segment on silver data
--    (demonstrates multi-dimension analysis beyond pre-built gold tables)
-- -----------------------------------------------------------------------------
SELECT
    UPPER(TRIM(fuel_type))    AS fuel_type,
    UPPER(TRIM(segment))      AS segment,
    COUNT(policy_id)          AS total_policies,
    SUM(claim_status)         AS total_claims,
    ROUND(AVG(claim_status), 4) AS claim_rate
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY UPPER(TRIM(fuel_type)), UPPER(TRIM(segment))
HAVING COUNT(policy_id) >= 50
ORDER BY claim_rate DESC
LIMIT 15;


-- -----------------------------------------------------------------------------
-- 9. Subscription length buckets vs claim rate
--    (ad-hoc analysis directly on silver — shows SQL beyond gold tables)
-- -----------------------------------------------------------------------------
SELECT
    CASE
        WHEN subscription_length < 3  THEN '0-2 months'
        WHEN subscription_length < 6  THEN '3-5 months'
        WHEN subscription_length < 9  THEN '6-8 months'
        WHEN subscription_length < 12 THEN '9-11 months'
        ELSE '12+ months'
    END AS subscription_bucket,
    COUNT(policy_id)                   AS total_policies,
    SUM(claim_status)                  AS total_claims,
    ROUND(AVG(claim_status), 4)        AS claim_rate
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY 1
ORDER BY claim_rate DESC;


-- -----------------------------------------------------------------------------
-- 10. Safety feature impact — comparing policies with vs without each feature
-- -----------------------------------------------------------------------------
SELECT
    'is_esc'              AS feature,
    is_esc                AS has_feature,
    COUNT(*)              AS policies,
    ROUND(AVG(claim_status), 4) AS claim_rate
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_esc

UNION ALL

SELECT
    'is_brake_assist'     AS feature,
    is_brake_assist       AS has_feature,
    COUNT(*)              AS policies,
    ROUND(AVG(claim_status), 4) AS claim_rate
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_brake_assist

UNION ALL

SELECT
    'is_parking_sensors'  AS feature,
    is_parking_sensors    AS has_feature,
    COUNT(*)              AS policies,
    ROUND(AVG(claim_status), 4) AS claim_rate
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_parking_sensors

ORDER BY feature, has_feature;
