-- =============================================================================
-- Databricks Dashboard Queries
-- Create each query in SQL Editor, then add to a Dashboard
-- =============================================================================

-- ===================== QUERY 1: KPI Summary Cards ============================
-- Visualization: Counter / KPI cards
-- Create 4 separate counter widgets from this single query
SELECT
    COUNT(policy_id) AS total_policies,
    SUM(claim_status) AS total_claims,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct,
    ROUND(AVG(subscription_length), 1) AS avg_subscription_months
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`;


-- ===================== QUERY 2: Claim Rate by Region (Top 10) ================
-- Visualization: Horizontal Bar Chart
-- X-axis: claim_rate | Y-axis: region_code | Sort: descending
SELECT
    region_code,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_region`
WHERE total_policies >= 100
ORDER BY claim_rate DESC
LIMIT 10;


-- ===================== QUERY 3: Claim Rate by Fuel Type ======================
-- Visualization: Vertical Bar Chart (colored bars)
-- X-axis: fuel_type | Y-axis: claim_rate_pct
SELECT
    fuel_type,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_fuel_type`
ORDER BY claim_rate DESC;


-- ===================== QUERY 4: Claim Rate by Customer Age Band ==============
-- Visualization: Bar + Line combo OR grouped bar chart
-- X-axis: customer_age_band | Y-axis: claim_rate_pct + total_policies
SELECT
    customer_age_band,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct,
    ROUND(total_policies * 100.0 / SUM(total_policies) OVER (), 1) AS portfolio_share_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_customer_age_band`
ORDER BY
    CASE customer_age_band
        WHEN 'Under 25' THEN 1
        WHEN '25-34' THEN 2
        WHEN '35-44' THEN 3
        WHEN '45-54' THEN 4
        WHEN '55+' THEN 5
    END;


-- ===================== QUERY 5: Claim Rate by Vehicle Age Band ===============
-- Visualization: Vertical Bar Chart
-- X-axis: vehicle_age_band | Y-axis: claim_rate_pct
SELECT
    vehicle_age_band,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_vehicle_age_band`
ORDER BY
    CASE vehicle_age_band
        WHEN '0-1' THEN 1
        WHEN '2-3' THEN 2
        WHEN '4-5' THEN 3
        WHEN '5+' THEN 4
    END;


-- ===================== QUERY 6: NCAP Safety Rating vs Claim Rate =============
-- Visualization: Line Chart (shows trend — higher NCAP = lower claims?)
-- X-axis: ncap_rating | Y-axis: claim_rate_pct
SELECT
    ncap_rating,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_ncap_rating`
ORDER BY ncap_rating;


-- ===================== QUERY 7: Segment Distribution =========================
-- Visualization: Pie / Donut Chart
-- Slice: segment | Value: total_policies
SELECT
    segment,
    total_policies,
    total_claims,
    ROUND(claim_rate * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_segment`
ORDER BY total_policies DESC;


-- ===================== QUERY 8: Fuel Type x Segment Heatmap ==================
-- Visualization: Pivot Table or Heatmap
-- Rows: fuel_type | Columns: segment | Values: claim_rate_pct
SELECT
    UPPER(TRIM(fuel_type)) AS fuel_type,
    UPPER(TRIM(segment)) AS segment,
    COUNT(policy_id) AS total_policies,
    SUM(claim_status) AS total_claims,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY UPPER(TRIM(fuel_type)), UPPER(TRIM(segment))
HAVING COUNT(policy_id) >= 50
ORDER BY claim_rate_pct DESC;


-- ===================== QUERY 9: Safety Features Impact =======================
-- Visualization: Grouped Bar Chart (has feature vs no feature)
SELECT
    'ESC' AS feature,
    is_esc AS has_feature,
    COUNT(*) AS policies,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_esc
UNION ALL
SELECT
    'Brake Assist' AS feature,
    is_brake_assist AS has_feature,
    COUNT(*) AS policies,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_brake_assist
UNION ALL
SELECT
    'Parking Sensors' AS feature,
    is_parking_sensors AS has_feature,
    COUNT(*) AS policies,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_parking_sensors
UNION ALL
SELECT
    'TPMS' AS feature,
    is_tpms AS has_feature,
    COUNT(*) AS policies,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY is_tpms
ORDER BY feature, has_feature;


-- ===================== QUERY 10: Subscription Length vs Claims ================
-- Visualization: Bar Chart
-- X-axis: subscription_bucket | Y-axis: claim_rate_pct
SELECT
    CASE
        WHEN subscription_length < 3  THEN '0-2 months'
        WHEN subscription_length < 6  THEN '3-5 months'
        WHEN subscription_length < 9  THEN '6-8 months'
        WHEN subscription_length < 12 THEN '9-11 months'
        ELSE '12+ months'
    END AS subscription_bucket,
    COUNT(policy_id) AS total_policies,
    SUM(claim_status) AS total_claims,
    ROUND(AVG(claim_status) * 100, 2) AS claim_rate_pct
FROM delta.`/Volumes/workspace/default/silver/insurance_policy_data_clean`
GROUP BY 1
ORDER BY
    CASE
        WHEN subscription_length < 3  THEN 1
        WHEN subscription_length < 6  THEN 2
        WHEN subscription_length < 9  THEN 3
        WHEN subscription_length < 12 THEN 4
        ELSE 5
    END;
