# Power BI Dashboard — Setup & Design Guide

## Connecting Power BI to Databricks

### Prerequisites
- Power BI Desktop (free download from Microsoft)
- Your Databricks workspace URL: `https://dbc-f984a51b-410e.cloud.databricks.com`
- A Databricks Personal Access Token (PAT)

### Step 1: Generate a Databricks PAT
1. In Databricks, click your profile icon (top-right) → **Settings**
2. Go to **Developer** → **Access tokens**
3. Click **Generate new token** → set expiry to 90 days → **Generate**
4. Copy the token — you'll need it in Power BI

### Step 2: Connect Power BI Desktop
1. Open Power BI Desktop
2. Click **Get Data** → search for **Databricks** → select **Azure Databricks**
3. Enter:
   - **Server Hostname**: `dbc-f984a51b-410e.cloud.databricks.com`
   - **HTTP Path**: (find this in Databricks → SQL Warehouses → your warehouse → Connection details → HTTP path)
4. Click **OK** → choose **Personal Access Token** → paste your PAT
5. Navigator will show your catalogs → expand **workspace** → **default**

### Step 3: Load Gold Tables
Select these tables from the navigator:
- `claim_rate_by_region`
- `claim_rate_by_segment`
- `claim_rate_by_fuel_type`
- `claim_rate_by_vehicle_age_band`
- `claim_rate_by_customer_age_band`
- `claim_rate_by_ncap_rating`
- `portfolio_summary`

Click **Load**.

If gold tables don't appear as managed tables, use **DirectQuery** with SQL:
1. Get Data → Databricks → Advanced → paste SQL:
```sql
SELECT * FROM delta.`/Volumes/workspace/default/gold/claim_rate_by_region`
```
Repeat for each gold table.

---

## Dashboard Layout Design

### Page 1: Executive Summary
```
┌──────────────────────────────────────────────────────────┐
│  AUTO INSURANCE RISK ANALYTICS                           │
│  Portfolio Overview Dashboard                            │
├──────────┬──────────┬──────────┬─────────────────────────┤
│ Total    │ Total    │ Claim    │ Avg Subscription        │
│ Policies │ Claims   │ Rate %   │ Length (months)          │
│ [KPI]    │ [KPI]    │ [KPI]    │ [KPI]                   │
├──────────┴──────────┴──────────┴─────────────────────────┤
│                                                          │
│  Claim Rate by Region (Top 10)     │  Claim Rate by      │
│  [Horizontal Bar Chart]            │  Fuel Type           │
│                                    │  [Donut Chart]       │
│                                    │                      │
├────────────────────────────────────┴──────────────────────┤
│                                                          │
│  Claim Rate by Customer Age Band   │  Claim Rate by      │
│  [Column Chart + Line overlay]     │  Vehicle Age Band   │
│                                    │  [Column Chart]     │
│                                    │                      │
└──────────────────────────────────────────────────────────┘
```

### Page 2: Risk Deep Dive
```
┌──────────────────────────────────────────────────────────┐
│  RISK ANALYSIS                                           │
│  Segment & Safety Feature Insights                       │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Claim Rate by Segment             │  NCAP Safety Rating │
│  [Bar Chart]                       │  vs Claim Rate      │
│                                    │  [Line Chart]       │
│                                    │                      │
├────────────────────────────────────┴──────────────────────┤
│                                                          │
│  Safety Feature Impact                                   │
│  [Clustered Bar: has feature vs no feature]              │
│  ESC | Brake Assist | Parking Sensors | TPMS             │
│                                                          │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Fuel Type × Segment Cross Analysis                      │
│  [Matrix / Heatmap]                                      │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Visualization Specifications

### KPI Cards (Page 1, top row)
- **Total Policies**: Format as whole number with comma separator
- **Total Claims**: Format as whole number
- **Claim Rate %**: Format as percentage with 2 decimals
- **Avg Subscription**: Format as 1 decimal + "months"
- Use conditional formatting: green if claim rate < 5%, amber 5-10%, red > 10%

### Claim Rate by Region (Horizontal Bar)
- Source: `claim_rate_by_region`
- Y-axis: `region_code` (sorted by claim_rate descending)
- X-axis: `claim_rate` (formatted as %)
- Filter: `total_policies >= 100` (credibility threshold)
- Data labels: ON
- Top 10 only

### Claim Rate by Fuel Type (Donut Chart)
- Source: `claim_rate_by_fuel_type`
- Legend: `fuel_type`
- Values: `total_policies` (for donut slices)
- Detail labels: show `claim_rate` as %
- Centre label: overall claim rate

### Claim Rate by Customer Age Band (Column + Line)
- Source: `claim_rate_by_customer_age_band`
- X-axis: `customer_age_band` (ordered: Under 25, 25-34, 35-44, 45-54, 55+)
- Column Y-axis: `total_policies` (left axis)
- Line Y-axis: `claim_rate` as % (right axis)
- This shows both volume and risk in one chart

### NCAP Rating vs Claim Rate (Line Chart)
- Source: `claim_rate_by_ncap_rating`
- X-axis: `ncap_rating` (0 to 5)
- Y-axis: `claim_rate` as %
- Add data point labels
- This should show whether safer cars (higher NCAP) have fewer claims

### Safety Feature Impact (Clustered Bar)
- Source: custom query (see dashboard_queries.sql, Query 9)
- X-axis: feature name
- Grouped by: has_feature (0 = No, 1 = Yes)
- Y-axis: claim_rate as %
- Color: green for has_feature=1, red for has_feature=0

---

## Color Palette Recommendation
- Primary: `#1B4F72` (dark blue — insurance/trust)
- Secondary: `#2E86C1` (medium blue)
- Accent: `#E74C3C` (red — for high-risk highlights)
- Success: `#27AE60` (green — for low-risk)
- Background: `#FAFAFA` (off-white)
- Text: `#2C3E50` (dark gray)

---

## Power BI Measures (DAX)

Create these measures for dynamic KPIs:

```
Overall Claim Rate = 
DIVIDE(
    SUM('portfolio_summary'[total_claims]),
    SUM('portfolio_summary'[total_policies]),
    0
)

Claim Rate Formatted = 
FORMAT([Overall Claim Rate], "0.00%")

Risk Category = 
SWITCH(
    TRUE(),
    [Overall Claim Rate] < 0.05, "Low Risk",
    [Overall Claim Rate] < 0.10, "Medium Risk",
    "High Risk"
)
```

---

## Tips for Portfolio Impact
- Add your name and "Data Analyst" in the footer
- Include a "Data Source" text box: "Source: Kaggle Auto Insurance Dataset | Pipeline: Databricks Delta Lake"
- Export as PDF and add to your GitHub repo docs/ folder
- Take a screenshot for your README
