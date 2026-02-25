-- Created a dedicated database to logically isolate marketing analytics workloads
CREATE DATABASE marketing_analytics;
USE marketing_analytics;

-- Brand master table capturing brand identity and onboarding details
CREATE TABLE brands (
    brand_id VARCHAR(10) PRIMARY KEY,
    brand_name VARCHAR(100),
    industry VARCHAR(50),
    city VARCHAR(50),
    contact_email VARCHAR(100),
    onboard_date DATE
);

-- Influencer dimension table capturing platform reach and engagement quality
CREATE TABLE influencers (
    influencer_id VARCHAR(10) PRIMARY KEY,
    influencer_name VARCHAR(100),
    platform VARCHAR(30),
    category VARCHAR(50),
    followers_count INT,
    city VARCHAR(50),
    engagement_rate DECIMAL(5,2),
    high_engagement_flag VARCHAR(10)
);


-- Campaign transaction table linking brands and influencers
CREATE TABLE campaigns (
    campaign_id VARCHAR(10) PRIMARY KEY,
    brand_id VARCHAR(10),
    influencer_id VARCHAR(10),
    campaign_type VARCHAR(30),
    campaign_start_date DATE,
    campaign_end_date DATE,
    campaign_budget DECIMAL(12,2),
    campaign_status VARCHAR(20),
    campaign_duration_days INT,
    is_repeat_brand VARCHAR(10),

    FOREIGN KEY (brand_id) REFERENCES brands(brand_id),
    FOREIGN KEY (influencer_id) REFERENCES influencers(influencer_id)
);


-- Performance fact table capturing daily campaign-level metrics
CREATE TABLE performance (
    performance_id VARCHAR(15) PRIMARY KEY,
    campaign_id VARCHAR(10),
    date DATE,
    impressions INT,
    clicks INT,
    likes INT,
    comments INT,
    shares INT,
    conversions INT,
    revenue_generated DECIMAL(12,2),
    roi_flag VARCHAR(10),
    revenue_anomaly VARCHAR(5),
    zero_engagement_flag VARCHAR(5),

    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);

-- Payment table to track actual spend realization
CREATE TABLE payments (
    payment_id VARCHAR(10) PRIMARY KEY,
    campaign_id VARCHAR(10),
    payment_date DATE,
    payment_mode VARCHAR(30),
    payment_status VARCHAR(20),
    amount_paid DECIMAL(12,2),

    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);


-- Data Quality & Integrity Checks (Pre-Analytics Validation)
SELECT COUNT(*) FROM campaigns
-- Checking orphan brand references in campaigns
SELECT DISTINCT brand_id
FROM campaigns
WHERE brand_id NOT IN (SELECT brand_id FROM brands);

-- Checking orphan influencer references in campaigns
SELECT DISTINCT influencer_id
FROM campaigns
WHERE influencer_id NOT IN (SELECT influencer_id FROM influencers);

-- Checking NULL budgets which could break ROI calculations
SELECT *
FROM campaigns
WHERE campaign_budget IS NULL;

-- Verify total payment records
SELECT COUNT(*) FROM payments;

-- Identify orphan payments not linked to any campaign
SELECT DISTINCT campaign_id
FROM payments
WHERE campaign_id NOT IN (SELECT campaign_id FROM campaigns);

-- Sanity Checks
-- Row count validation across all core entities
SELECT COUNT(*) FROM brands;
SELECT COUNT(*) FROM campaigns;
SELECT COUNT(*) FROM payments;
SELECT COUNT(*) FROM influencers;
SELECT COUNT(*) FROM performance;


-- Detect duplicate campaign IDs (should not exist due to primary key)
SELECT campaign_id, COUNT(*)
FROM campaigns
GROUP BY campaign_id
HAVING COUNT(*) > 1;

-- Identify negative revenue values
SELECT *
FROM performance
WHERE revenue_generated < 0;

-- Revenue recorded without impressions (logical inconsistency)
SELECT *
FROM performance
WHERE impressions = 0 AND revenue_generated > 0;


-- Row count reconciliation across base tables and analytical views
SELECT 'brands' AS table_name, COUNT(*) AS total_rows FROM brands
UNION ALL
SELECT 'campaigns', COUNT(*) FROM campaigns
UNION ALL
SELECT 'performance', COUNT(*) FROM performance
UNION ALL
SELECT 'payments', COUNT(*) FROM payments
UNION ALL
SELECT 'influencers', COUNT(*) FROM influencers
UNION ALL
SELECT 'vw_campaign_effectiveness', COUNT(*) FROM vw_campaign_effectiveness
UNION ALL
SELECT 'vw_executive_analytics', COUNT(*) FROM vw_executive_analytics;

-- Critical NULL Checks on Key Columns
-- Validate NULLs in business-critical fields
SELECT 'brands' AS table_name,
       COUNT(*) AS total_rows,
       SUM(CASE WHEN brand_id IS NULL OR brand_name IS NULL THEN 1 ELSE 0 END) AS critical_null_rows
FROM brands
UNION ALL
SELECT 'campaigns',
       COUNT(*),
       SUM(CASE WHEN campaign_id IS NULL OR brand_id IS NULL OR influencer_id IS NULL THEN 1 ELSE 0 END)
FROM campaigns
UNION ALL
SELECT 'performance',
       COUNT(*),
       SUM(CASE WHEN campaign_id IS NULL OR date IS NULL THEN 1 ELSE 0 END)
FROM performance
UNION ALL
SELECT 'payments',
       COUNT(*),
       SUM(CASE WHEN campaign_id IS NULL OR amount_paid IS NULL THEN 1 ELSE 0 END)
FROM payments
UNION ALL
SELECT 'influencers',
       COUNT(*),
       SUM(CASE WHEN influencer_id IS NULL OR influencer_name IS NULL THEN 1 ELSE 0 END)
FROM influencers
UNION ALL
SELECT 'vw_campaign_effectiveness',
       COUNT(*),
       SUM(CASE WHEN campaign_id IS NULL OR revenue_generated IS NULL THEN 1 ELSE 0 END)
FROM vw_campaign_effectiveness
UNION ALL
SELECT 'vw_executive_analytics',
       COUNT(*),
       SUM(CASE WHEN campaign_id IS NULL OR revenue_generated IS NULL THEN 1 ELSE 0 END)
FROM vw_executive_analytics;



-- Marketing Analytics Layer: Unified Performance View
-- Created a unified analytical view joining core marketing entities: 
-- performance, campaigns, brands, influencers.
-- This serves as a single source of truth for all executive-level KPIs
-- and ensures consistency across dashboards and reports.

CREATE OR REPLACE VIEW vw_executive_analytics AS
SELECT
    p.date,
    p.performance_id,
    p.campaign_id,

    c.brand_id,
    b.brand_name,
    b.industry,
    b.city AS brand_city,

    c.influencer_id,
    i.influencer_name,
    i.platform,
    i.category AS influencer_category,
    i.followers_count,
    i.engagement_rate,

    c.campaign_type,
    c.campaign_status,
    c.campaign_budget,
    c.campaign_start_date,
    c.campaign_end_date,
    c.campaign_duration_days,
    c.is_repeat_brand,

    p.impressions,
    p.clicks,
    p.likes,
    p.comments,
    p.shares,
    p.conversions,
    p.revenue_generated,
    p.roi_flag,
    p.revenue_anomaly,
    p.zero_engagement_flag

FROM performance p
JOIN campaigns c   ON p.campaign_id = c.campaign_id
JOIN brands b      ON c.brand_id = b.brand_id
JOIN influencers i ON c.influencer_id = i.influencer_id;

-- Core Financial KPIs

-- Total revenue generated across all campaigns
SELECT SUM(revenue_generated) AS total_revenue
FROM vw_executive_analytics;

-- Count of unique campaigns launched
SELECT COUNT(DISTINCT campaign_id) AS total_campaigns
FROM vw_executive_analytics;

-- Total marketing investment across campaigns (avoiding double-counting with DISTINCT)
SELECT SUM(DISTINCT campaign_budget) AS total_investment
FROM vw_executive_analytics;

-- Overall ROI % for the marketing portfolio
SELECT 
ROUND(
(SUM(revenue_generated) - SUM(DISTINCT campaign_budget)) 
/ SUM(DISTINCT campaign_budget) * 100, 2
) AS overall_roi_pct
FROM vw_executive_analytics;

-- Influencer network size (unique active influencers)
SELECT COUNT(DISTINCT influencer_id) AS influencer_network
FROM vw_executive_analytics;

-- Brand retention rate: % of campaigns from repeat brands
SELECT 
ROUND(
SUM(CASE WHEN is_repeat_brand = 'Yes' THEN 1 ELSE 0 END)
/
COUNT(*) * 100,
2
) AS brand_retention_pct
FROM campaigns;

-- Advanced Marketing Insights(KPIs)

 -- Revenue Efficiency Index (REI): ratio-based efficiency metric
SELECT 
ROUND(SUM(revenue_generated) / SUM(DISTINCT campaign_budget), 3) 
AS revenue_efficiency_index
FROM vw_executive_analytics;

-- Revenue Volatility Score: measures stability of revenue
SELECT 
ROUND(
STDDEV_POP(revenue_generated) / AVG(revenue_generated),
2
) AS revenue_volatility_score
FROM vw_executive_analytics;

-- Revenue Concentration Risk: dependency on a single campaign
WITH campaign_rev AS (
    SELECT 
        campaign_id, 
        SUM(revenue_generated) AS rev
    FROM vw_executive_analytics
    GROUP BY campaign_id
)
SELECT 
ROUND(MAX(rev) / SUM(rev) * 100, 2) 
AS revenue_concentration_percent
FROM campaign_rev;

-- Campaign failure ratio: % of campaigns with zero revenue
SELECT
ROUND(
COUNT(DISTINCT CASE WHEN revenue_generated = 0 THEN campaign_id END)
/
COUNT(DISTINCT campaign_id) * 100,
2
) AS campaign_failure_ratio
FROM vw_executive_analytics;

-- Marketing capital exposed in non-active campaigns (Cancelled/Paused)
SELECT 
SUM(DISTINCT campaign_budget) AS capital_exposed
FROM vw_executive_analytics
WHERE campaign_status IN ('Cancelled','Paused');

-- High-Level Revenue Signal: textual KPI for dashboard
SELECT
CASE
    WHEN SUM(revenue_generated) / SUM(DISTINCT campaign_budget) >= 1.3
         AND STDDEV(revenue_generated)/AVG(revenue_generated) < 0.5
    THEN 'STRONG & STABLE'
    WHEN SUM(revenue_generated) / SUM(DISTINCT campaign_budget) >= 1
    THEN 'PROFITABLE BUT VOLATILE'
    ELSE 'HIGH RISK'
END AS executive_revenue_signal
FROM vw_executive_analytics;

-- Revenue Coverage Index: % of campaigns generating revenue
SELECT
ROUND(
COUNT(DISTINCT CASE WHEN revenue_generated > 0 THEN campaign_id END)
/
COUNT(DISTINCT campaign_id) * 100,
2
) AS revenue_coverage_index
FROM vw_executive_analytics;

-- Business Scalability Score: measures efficiency of brand-influencer network vs campaigns
SELECT
ROUND(
(COUNT(DISTINCT brand_id) * COUNT(DISTINCT influencer_id)) /
COUNT(DISTINCT campaign_id),
2
) AS business_scalability_score
FROM vw_executive_analytics;


-- Charting & Visualization Queries

-- Daily revenue trend (time series)
SELECT date, SUM(revenue_generated) AS daily_revenue
FROM vw_executive_analytics
GROUP BY date
ORDER BY date;

-- Campaign status distribution for dashboards
SELECT campaign_status, COUNT(DISTINCT campaign_id) AS campaign_count
FROM vw_executive_analytics
GROUP BY campaign_status;



-- Page 2: Campaign Effectiveness Layer
-- Master analytical view combining campaign performance and budget data
-- This view calculates core efficiency metrics (CTR, conversions, ROI)
-- and serves as the foundation for campaign-level KPIs and dashboards.
CREATE OR REPLACE VIEW vw_campaign_effectiveness AS
SELECT
    p.date,
    p.performance_id,
    p.campaign_id,

    c.brand_id,
    c.influencer_id,
    c.campaign_type,
    c.campaign_status,
    c.campaign_budget,

    p.impressions,
    p.clicks,
    p.conversions,
    p.revenue_generated,

    -- Core Efficiency Metrics
    ROUND(p.clicks / NULLIF(p.impressions,0) * 100, 2) AS ctr_pct,
    ROUND(p.conversions / NULLIF(p.clicks,0) * 100, 2) AS conversion_rate_pct,
    ROUND(p.revenue_generated / NULLIF(c.campaign_budget,0), 2) AS roi_ratio

FROM performance p
JOIN campaigns c ON p.campaign_id = c.campaign_id;



-- Core Campaign KPIs
-- Total conversions generated across all campaigns
SELECT SUM(conversions) AS total_conversions
FROM vw_campaign_effectiveness;

-- Total clicks generated
SELECT SUM(clicks) AS total_clicks
FROM vw_campaign_effectiveness;

-- Overall conversion rate % across campaigns
SELECT 
ROUND(
SUM(conversions) / NULLIF(SUM(clicks),0) * 100,
2
) AS overall_conversion_rate_pct
FROM vw_campaign_effectiveness;

-- Cost per conversion across campaigns
SELECT 
ROUND(
SUM(campaign_budget) / NULLIF(SUM(conversions),0),
2
) AS cost_per_conversion
FROM vw_campaign_effectiveness;

-- Revenue trends over time (MoM growth)
SELECT
date,
ROUND(
(
SUM(revenue_generated) -
LAG(SUM(revenue_generated)) OVER (ORDER BY date)
)
/
NULLIF(LAG(SUM(revenue_generated)) OVER (ORDER BY date),0) * 100,
2
) AS revenue_mom_growth_pct
FROM vw_campaign_effectiveness
GROUP BY date
ORDER BY date;

-- Revenue per campaign
SELECT
campaign_id,
ROUND(SUM(revenue_generated),2) AS revenue_per_campaign
FROM vw_campaign_effectiveness
GROUP BY campaign_id;

-- Charting & Visualization Queries
-- Top campaigns by revenue contribution
SELECT
campaign_id,
ROUND(SUM(revenue_generated),2) AS total_revenue
FROM vw_campaign_effectiveness
GROUP BY campaign_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Daily conversion rate trend
SELECT
date,
ROUND(
SUM(conversions) / NULLIF(SUM(clicks),0) * 100,
2
) AS daily_conversion_rate_pct
FROM vw_campaign_effectiveness
GROUP BY date
ORDER BY date;

-- Campaign spend vs conversion efficiency
SELECT
campaign_id,
campaign_budget,
ROUND(
SUM(conversions) / NULLIF(SUM(clicks),0) * 100,
2
) AS conversion_efficiency_pct
FROM vw_campaign_effectiveness
GROUP BY campaign_id, campaign_budget;


-- Advanced Conversion & Efficiency Insights
-- Conversion waste ratio: % of clicks not converted
SELECT
ROUND(
(SUM(clicks) - SUM(conversions)) / NULLIF(SUM(clicks),0) * 100,
2
) AS conversion_waste_ratio_pct
FROM vw_campaign_effectiveness;

-- Revenue per click: monetary efficiency of clicks
SELECT
ROUND(
SUM(revenue_generated) / NULLIF(SUM(clicks),0),
2
) AS revenue_per_click_power
FROM vw_campaign_effectiveness;

-- Conversion leverage index: weighted impact of conversions on revenue
SELECT
ROUND(
(SUM(conversions) * SUM(revenue_generated)) 
/
NULLIF(SUM(clicks),0),
2
) AS conversion_leverage_index
FROM vw_campaign_effectiveness;

-- Revenue dependence index: systematic risk due to few high-performing campaigns
WITH campaign_rev AS (
    SELECT campaign_id, SUM(revenue_generated) AS rev
    FROM vw_campaign_effectiveness
    GROUP BY campaign_id
)
SELECT
ROUND(
SUM(CASE WHEN rev > (SELECT AVG(rev) FROM campaign_rev) THEN rev ELSE 0 END)
/
SUM(rev) * 100,
2
) AS revenue_dependence_index
FROM campaign_rev;

-- Campaign efficiency frontier score
SELECT
ROUND(
AVG(conversion_rate_pct) * AVG(roi_ratio) * 
(AVG(revenue_generated) / AVG(campaign_budget)),
2
) AS campaign_efficiency_frontier_score
FROM vw_campaign_effectiveness;

-- Profit reliability index (stability + profitability)
SELECT
ROUND(
AVG(roi_ratio) / NULLIF(AVG(revenue_instability_score),0),
2
) AS profit_reliability_index
FROM (
    SELECT
    campaign_id,
    AVG(roi_ratio) AS roi_ratio,
    STDDEV_POP(revenue_generated) / AVG(revenue_generated) 
        AS revenue_instability_score
    FROM vw_campaign_effectiveness
    GROUP BY campaign_id
) t;

-- Optimization opportunity score per campaign
SELECT
campaign_id,
ROUND(
conversion_rate_pct * roi_ratio,
2
) AS optimization_score
FROM vw_campaign_effectiveness
ORDER BY optimization_score DESC;

-- Conversion momentum index: average daily growth in conversions
SELECT
ROUND(AVG(daily_growth_pct),2) AS conversion_momentum_index
FROM (
    SELECT
    date,
    ROUND(
    (
    SUM(conversions) -
    LAG(SUM(conversions)) OVER (ORDER BY date)
    )
    /
    NULLIF(LAG(SUM(conversions)) OVER (ORDER BY date),0) * 100,
    2
    ) AS daily_growth_pct
    FROM vw_campaign_effectiveness
    GROUP BY date
) t;

-- Campaign cumulative revenue curve (maturity curve)
-- Recursive CTE 

WITH RECURSIVE campaign_days AS (
    SELECT
        campaign_id,
        campaign_start_date AS day_date,
        campaign_end_date
    FROM campaigns

    UNION ALL

    SELECT
        campaign_id,
        DATE_ADD(day_date, INTERVAL 1 DAY),
        campaign_end_date
    FROM campaign_days
    WHERE day_date < campaign_end_date
),
daily_perf AS (
    SELECT
        cd.campaign_id,
        cd.day_date,
        COALESCE(SUM(p.revenue_generated),0) AS daily_revenue
    FROM campaign_days cd
    LEFT JOIN performance p
        ON cd.campaign_id = p.campaign_id
       AND cd.day_date = p.date
    GROUP BY cd.campaign_id, cd.day_date
)
SELECT
campaign_id,
day_date,
SUM(daily_revenue) OVER (PARTITION BY campaign_id ORDER BY day_date) 
AS cumulative_revenue
FROM daily_perf;


-- Stored procedure: automated campaign health summary
DELIMITER $$

CREATE PROCEDURE sp_campaign_effectiveness_summary()
BEGIN
    SELECT
        COUNT(DISTINCT campaign_id) AS total_campaigns,
        ROUND(SUM(revenue_generated),2) AS total_revenue,
        ROUND(AVG(roi_ratio),2) AS avg_roi,
        ROUND(AVG(conversion_rate_pct),2) AS avg_conversion_rate,
        ROUND(
            COUNT(DISTINCT CASE WHEN roi_ratio < 1 THEN campaign_id END)
            /
            COUNT(DISTINCT campaign_id) * 100,
        2) AS loss_campaign_pct,
        ROUND(
            COUNT(DISTINCT CASE WHEN roi_ratio >= 1.5 THEN campaign_id END)
            /
            COUNT(DISTINCT campaign_id) * 100,
        2) AS high_performer_pct
    FROM vw_campaign_effectiveness;
END$$

DELIMITER ;
CALL sp_campaign_effectiveness_summary();


-- Conversion & revenue diagnostic signal (text KPI)
SELECT
CASE
    WHEN ROUND(MAX(rev) / SUM(rev) * 100,2) > 40
    THEN 'REVENUE HIGHLY CONCENTRATED – OPTIMIZATION REQUIRED'

    WHEN ROUND(SUM(campaign_budget)/SUM(conversions),2) > 500
    THEN 'COST PER CONVERSION CRITICAL – SPEND INEFFICIENCY DETECTED'

    WHEN AVG(roi_ratio) >= 1.3 
         AND AVG(conversion_rate_pct) > 5
    THEN 'HIGHLY OPTIMIZED CONVERSION ENGINE'

    ELSE 'PERFORMANCE STABLE WITH OPTIMIZATION POTENTIAL'
END AS conversion_revenue_diagnostic_signal
FROM (
    SELECT 
        campaign_id,
        SUM(revenue_generated) AS rev,
        SUM(campaign_budget) AS campaign_budget,
        SUM(conversions) AS conversions,
        AVG(roi_ratio) AS roi_ratio,
        AVG(conversion_rate_pct) AS conversion_rate_pct
    FROM vw_campaign_effectiveness
    GROUP BY campaign_id
) t;
-- Page 3: Brand & Influencer Value Analysis
-- Consolidated analytical view combining brand, influencer and campaign
-- performance data. This view helps evaluate which brand–influencer
-- partnerships drive revenue, engagement and repeat value.

CREATE VIEW vw_brand_influencer_value AS
SELECT
    c.campaign_id,
    c.brand_id,
    b.brand_name,
    c.influencer_id,
    i.influencer_name,
    i.platform,
    i.engagement_rate,
    i.followers_count,
    c.campaign_budget,
    p.revenue_generated,
    p.impressions,
    p.clicks,
    p.conversions,
    p.likes,
    p.comments,
    p.shares,
    c.is_repeat_brand,
    c.campaign_status
FROM campaigns c
JOIN brands b ON c.brand_id = b.brand_id
JOIN influencers i ON c.influencer_id = i.influencer_id
JOIN performance p ON c.campaign_id = p.campaign_id;


-- Core Brand & Influencer KPIs
-- Number of active brands generating revenue
SELECT COUNT(DISTINCT brand_id) AS total_active_brands
FROM vw_brand_influencer_value
WHERE revenue_generated > 0;

-- Repeat brand campaign rate (% of campaigns from returning brands)
SELECT
ROUND(
SUM(CASE WHEN is_repeat_brand='Yes' THEN 1 ELSE 0 END)
/ COUNT(*) * 100, 2
) AS repeat_brand_campaign_rate
FROM vw_brand_influencer_value;

-- Revenue contribution per campaign
SELECT
campaign_id,
ROUND(SUM(revenue_generated),2) AS revenue_per_campaign
FROM vw_brand_influencer_value
GROUP BY campaign_id;

-- % of influencers delivering exceptional engagement (>8%)
SELECT
ROUND(
COUNT(DISTINCT CASE WHEN engagement_rate > 8 THEN influencer_id END)
/
COUNT(DISTINCT influencer_id) * 100, 2
) AS top_influencer_engagement_pct
FROM vw_brand_influencer_value;

-- Average engagement rate across campaigns
SELECT ROUND(AVG(engagement_rate),2) AS avg_engagement_rate
FROM vw_brand_influencer_value;

-- Deeper Value Indicators
-- Engagement efficiency: revenue generated per interaction
SELECT
influencer_id,
ROUND(SUM(revenue_generated)/NULLIF(SUM(likes + comments + shares),0),4) AS revenue_per_engagement
FROM vw_brand_influencer_value
GROUP BY influencer_id
ORDER BY revenue_per_engagement DESC;

-- Brand leadership score: rewards repeat-brand revenue
SELECT
    brand_id,
    ROUND(
        SUM(revenue_generated * CASE WHEN is_repeat_brand='Yes' THEN 1.2 ELSE 1 END),
    2) AS brand_leadership_score
FROM vw_brand_influencer_value
GROUP BY brand_id
ORDER BY brand_leadership_score DESC
LIMIT 1000;

-- Platform-level influence impact
SELECT
i.platform,
ROUND(SUM(p.revenue_generated),2) AS platform_revenue,
ROUND(AVG(i.engagement_rate),2) AS avg_platform_engagement
FROM vw_brand_influencer_value p
JOIN influencers i ON p.influencer_id = i.influencer_id
GROUP BY i.platform
ORDER BY platform_revenue DESC;

-- Revenue concentration risk by brand
WITH brand_rev AS (
    SELECT brand_id, SUM(revenue_generated) AS total_rev
    FROM vw_brand_influencer_value
    GROUP BY brand_id
)
SELECT
ROUND(MAX(total_rev)/SUM(total_rev)*100,2) AS top_brand_rev_pct
FROM brand_rev;

-- Visualization Queries
-- Top brands by revenue
SELECT brand_name, SUM(revenue_generated) AS total_revenue
FROM vw_brand_influencer_value
GROUP BY brand_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Top influencers by engagement rate
SELECT influencer_name, AVG(engagement_rate) AS avg_engagement
FROM vw_brand_influencer_value
GROUP BY influencer_name
ORDER BY avg_engagement DESC
LIMIT 10;

-- Influencer network distribution by platform
SELECT platform, COUNT(DISTINCT influencer_id) AS influencer_count
FROM vw_brand_influencer_value
GROUP BY platform;

-- Influencer revenue growth curve (cumulative impact over time)
-- Helps observe which influencers create early wins vs sustained value
WITH RECURSIVE influencer_days AS (
    SELECT influencer_id, campaign_id, MIN(campaign_start_date) AS day_date, MAX(campaign_end_date) AS end_date
    FROM campaigns
    GROUP BY influencer_id, campaign_id

    UNION ALL

    SELECT influencer_id, campaign_id, DATE_ADD(day_date, INTERVAL 1 DAY), end_date
    FROM influencer_days
    WHERE day_date < end_date
),
daily_engagement AS (
    SELECT id.influencer_id, id.day_date, COALESCE(SUM(p.revenue_generated),0) AS daily_revenue
    FROM influencer_days id
    LEFT JOIN performance p ON id.campaign_id = p.campaign_id AND id.day_date = p.date
    GROUP BY id.influencer_id, id.day_date
)
SELECT influencer_id, day_date,
SUM(daily_revenue) OVER(PARTITION BY influencer_id ORDER BY day_date) AS cumulative_revenue
FROM daily_engagement
ORDER BY influencer_id, day_date;



-- Page 4: Audience & Traffic Quality Analysis
-- Analytical view focused on audience behavior, interaction depth,
-- and traffic quality across campaigns. 
-- This layer helps evaluate whether reach is translating into 
-- meaningful engagement and conversions.

CREATE OR REPLACE VIEW vw_audience_traffic_quality AS
SELECT
    p.date,
    p.performance_id,
    p.campaign_id,
    c.brand_id,
    b.brand_name,
    c.influencer_id,
    i.influencer_name,
    i.platform,
    i.followers_count,
    p.impressions,
    p.clicks,
    p.likes,
    p.comments,
    p.shares,
    p.conversions,
    c.campaign_budget,
    c.campaign_type,
    c.campaign_status
FROM performance p
JOIN campaigns c ON p.campaign_id = c.campaign_id
JOIN brands b ON c.brand_id = b.brand_id
JOIN influencers i ON c.influencer_id = i.influencer_id;

-- Core Audience KPIs
-- Total campaign reach (overall visibility)
SELECT SUM(impressions) AS total_campaign_reach
FROM vw_audience_traffic_quality;

-- Total user engagement across campaigns
SELECT SUM(likes + comments + shares) AS total_user_engagement
FROM vw_audience_traffic_quality;

-- Engagement effectiveness rate: engagement vs reach
SELECT 
ROUND(SUM(likes + comments + shares) / NULLIF(SUM(impressions),0) * 100,2) AS engagement_effectiveness_pct
FROM vw_audience_traffic_quality;

-- Cost per click to measure traffic efficiency
SELECT 
ROUND(SUM(campaign_budget) / NULLIF(SUM(clicks),0),2) AS cost_per_click
FROM vw_audience_traffic_quality;


-- % of influencers generating strong interaction rates
SELECT 
ROUND(
    COUNT(DISTINCT CASE 
        WHEN (likes + comments + shares)/NULLIF(impressions,0)*100 > 5 
        THEN influencer_id 
    END)
    / COUNT(DISTINCT influencer_id) * 100,
2
) AS high_impact_influencer_pct
FROM vw_audience_traffic_quality;



-- Deeper Engagement & Quality Indicators
-- Traffic Quality Index: weighted engagement + conversions
SELECT
ROUND(
(SUM(likes + comments + shares) + SUM(conversions) * 2) / NULLIF(SUM(impressions),0),4
) AS traffic_quality_index
FROM vw_audience_traffic_quality;



-- Conversion depth: how engagement translates to conversions
SELECT
ROUND(
SUM(conversions) / NULLIF(SUM(likes + comments + shares),0),4
) AS conversion_depth_score
FROM vw_audience_traffic_quality;


-- Engagement per budget spent (campaign efficiency)
SELECT campaign_id,
       SUM(likes + comments + shares) AS total_engagement,
       SUM(campaign_budget) AS total_budget,
       ROUND(SUM(likes + comments + shares)/NULLIF(SUM(campaign_budget),0),2) AS engagement_per_budget
FROM vw_audience_traffic_quality
GROUP BY campaign_id
HAVING engagement_per_budget > 0.75
ORDER BY engagement_per_budget DESC;



-- Interaction efficiency per influencer follower base
SELECT influencer_id,
ROUND(SUM(likes + comments + shares) / NULLIF(SUM(followers_count),0),4) AS engagement_per_follower
FROM vw_audience_traffic_quality
GROUP BY influencer_id
ORDER BY engagement_per_follower DESC;


-- Engagement efficiency by campaign type
SELECT 
    campaign_type,
    ROUND(SUM(likes + comments + shares) / NULLIF(SUM(impressions),0), 4) AS engagement_per_impression
FROM vw_audience_traffic_quality
WHERE campaign_type IS NOT NULL
  AND TRIM(campaign_type) <> ''
GROUP BY campaign_type
ORDER BY engagement_per_impression DESC;



-- Cost-effectiveness score (engagement + conversions vs spend)
SELECT campaign_id,
ROUND(SUM(likes + comments + shares + conversions*2)/NULLIF(SUM(campaign_budget),0),2) AS cost_effectiveness_score
FROM vw_audience_traffic_quality
GROUP BY campaign_id
ORDER BY cost_effectiveness_score DESC;


-- Engagement buildup over campaign duration
WITH RECURSIVE campaign_days AS (
    SELECT campaign_id, MIN(date) AS day_date, MAX(date) AS end_date
    FROM vw_audience_traffic_quality
    GROUP BY campaign_id
    UNION ALL
    SELECT campaign_id, DATE_ADD(day_date, INTERVAL 1 DAY), end_date
    FROM campaign_days
    WHERE day_date < end_date
),
daily_engagement AS (
    SELECT cd.campaign_id, cd.day_date,
           COALESCE(SUM(likes + comments + shares),0) AS daily_engagement
    FROM campaign_days cd
    LEFT JOIN vw_audience_traffic_quality p
        ON cd.campaign_id = p.campaign_id AND cd.day_date = p.date
    GROUP BY cd.campaign_id, cd.day_date
)
SELECT campaign_id, day_date,
       SUM(daily_engagement) OVER(PARTITION BY campaign_id ORDER BY day_date) AS cumulative_engagement
FROM daily_engagement
ORDER BY campaign_id, day_date;


-- Stored procedure: quick engagement health snapshot
DELIMITER $$

CREATE PROCEDURE sp_engagement_health_summary()
BEGIN
    SELECT 
        COUNT(DISTINCT campaign_id) AS total_campaigns,
        ROUND(SUM(likes + comments + shares),2) AS total_engagement,
        ROUND(SUM(impressions),2) AS total_reach,
        ROUND(SUM(likes + comments + shares)/NULLIF(SUM(impressions),0)*100,2) AS engagement_effectiveness_pct,
        ROUND(SUM(likes + comments + shares + conversions*2)/NULLIF(SUM(campaign_budget),0)*100,2) AS avg_cost_effectiveness_score, -- scaled for readability
        ROUND(
            COUNT(DISTINCT CASE WHEN (likes + comments + shares)/NULLIF(campaign_budget,0) > 0.75 THEN campaign_id END)/
            COUNT(DISTINCT campaign_id)*100,2
        ) AS high_efficiency_campaign_pct -- threshold lowered to realistic value
    FROM vw_audience_traffic_quality;
END$$

DELIMITER ;

-- Call the procedure
CALL sp_engagement_health_summary();

-- Visualization Queries
-- Engagement depth vs reach
SELECT 
    campaign_id,
    SUM(impressions) AS total_reach,
    SUM(likes + comments + shares) AS total_engagement
FROM vw_audience_traffic_quality
GROUP BY campaign_id
ORDER BY total_reach DESC;

-- Cost efficiency vs conversion quality
SELECT 
    campaign_id,
    ROUND(SUM(campaign_budget)/SUM(conversions),2) AS cost_per_conversion,
    ROUND(SUM(conversions)/SUM(likes + comments + shares),4) AS conversion_depth_score
FROM vw_audience_traffic_quality
WHERE conversions > 0
  AND (likes + comments + shares) > 0
GROUP BY campaign_id
ORDER BY cost_per_conversion;

-- Conversion quality diagnostic (text KPI)
SELECT
CASE
    WHEN AVG(conversion_depth) < 0.03
    THEN 'ENGAGEMENT HIGH BUT CONVERSION QUALITY WEAK'
    WHEN AVG(cost_per_conversion) > 100
    THEN 'COST PER CONVERSION CRITICAL'
    ELSE 'CONVERSION QUALITY HEALTHY'
END AS conversion_quality_signal
FROM (
    SELECT 
        campaign_id,
        SUM(conversions) / NULLIF(SUM(likes + comments + shares),0) AS conversion_depth,
        SUM(campaign_budget) / NULLIF(SUM(conversions),0) AS cost_per_conversion
    FROM vw_audience_traffic_quality
    WHERE conversions > 0
      AND (likes + comments + shares) > 0
    GROUP BY campaign_id
) t;

-- Engagement trend over time
SELECT 
    date,
    SUM(likes + comments + shares) AS total_engagement,
    SUM(impressions) AS total_reach,
    ROUND(SUM(likes + comments + shares)/NULLIF(SUM(impressions),0)*100,2) AS engagement_effectiveness_pct
FROM vw_audience_traffic_quality
GROUP BY date
ORDER BY date;



-- High-impact influencer segmentation
-- Visualize how many influencers are driving exceptional engagement
SELECT 
    CASE 
        WHEN interaction_ratio >= 0.08 THEN 'High Impact'
        ELSE 'Others'
    END AS influencer_segment,
    COUNT(DISTINCT influencer_id) AS influencer_count
FROM (
    SELECT 
        influencer_id,
        SUM(likes + comments + shares) / NULLIF(SUM(impressions),0) AS interaction_ratio
    FROM vw_audience_traffic_quality
    GROUP BY influencer_id
) t
GROUP BY influencer_segment;

-- #Page5 Growth Signals & Scale Readiness

-- Base view for growth tracking
CREATE OR REPLACE VIEW vw_growth_strategy AS
SELECT
    date,
    campaign_id,
    campaign_type,
    campaign_budget,
    impressions,
    clicks,
    conversions,
    likes,
    comments,
    shares,
    (likes + comments + shares) AS total_engagement,

      -- revenue proxy assumption
    conversions * 50 AS revenue   -- assume avg ₹/$50 per conversion

FROM vw_audience_traffic_quality;


#core kpis
-- Revenue Growth Momentum (YoY)
WITH yearly_revenue AS (
    SELECT
        YEAR(date) AS year,
        SUM(revenue) AS total_revenue
    FROM vw_growth_strategy
    GROUP BY YEAR(date)
)
SELECT
    ROUND(
        (MAX(total_revenue) - MIN(total_revenue))
        / MIN(total_revenue) * 100, 2
    ) AS revenue_growth_yoy_pct
FROM yearly_revenue;


-- Year-to-Date Revenue
SELECT
    SUM(revenue) AS ytd_revenue
FROM vw_growth_strategy
WHERE YEAR(date) = (
    SELECT MAX(YEAR(date)) FROM vw_growth_strategy
);


-- Average Monthly Marketing Spend
SELECT
    ROUND(SUM(campaign_budget) / COUNT(DISTINCT MONTH(date)), 2)
    AS avg_monthly_marketing_spend
FROM vw_growth_strategy;


-- Revenue Stability (3-Month Avg)
SELECT
    DATE_FORMAT(date, '%Y-%m') AS month,
    ROUND(
        AVG(SUM(revenue)) OVER (
            ORDER BY DATE_FORMAT(date, '%Y-%m')
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS revenue_stability_index
FROM vw_growth_strategy
GROUP BY DATE_FORMAT(date, '%Y-%m');

-- Visualization Queries
-- Revenue vs Marketing Spend
SELECT
    DATE_FORMAT(date, '%Y-%m') AS month,
    SUM(revenue) AS total_revenue,
    SUM(campaign_budget) AS marketing_spend
FROM vw_growth_strategy
GROUP BY DATE_FORMAT(date, '%Y-%m')
ORDER BY month;


-- Cumulative Revenue Momentum (YTD Area Chart)
SELECT
    date,
    SUM(revenue) OVER (
        PARTITION BY YEAR(date)
        ORDER BY date
    ) AS cumulative_ytd_revenue
FROM vw_growth_strategy
WHERE YEAR(date) = (
    SELECT MAX(YEAR(date)) FROM vw_growth_strategy
)
ORDER BY date;

-- Audience Engagement - Conversion Funnel
SELECT
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(total_engagement) AS engagements,
    SUM(conversions) AS conversions
FROM vw_growth_strategy;

-- advanced kpis
-- Revenue Efficiency
SELECT
    ROUND(SUM(revenue) / NULLIF(SUM(campaign_budget),0), 2)
    AS revenue_efficiency_ratio
FROM vw_growth_strategy;


-- Funnel Drop-Off Rate
SELECT
    ROUND(
        (1 - SUM(conversions) / NULLIF(SUM(clicks),0)) * 100, 2
    ) AS funnel_leakage_pct
FROM vw_growth_strategy;

-- Engagement to Conversion Strength
SELECT
    ROUND(
        SUM(conversions) / NULLIF(SUM(total_engagement),0), 4
    ) AS engagement_conversion_strength
FROM vw_growth_strategy;

-- Campaign Scalability Score
SELECT
    campaign_type,
    ROUND(SUM(revenue) / COUNT(DISTINCT campaign_id), 2) AS scalability_score
FROM vw_growth_strategy
WHERE campaign_type IS NOT NULL
  AND campaign_type <> ''
GROUP BY campaign_type
ORDER BY scalability_score DESC;

-- Profitable Campaign Share
SELECT
ROUND(
COUNT(DISTINCT CASE WHEN revenue > campaign_budget THEN campaign_id END)
/ COUNT(DISTINCT campaign_id) * 100,2
) AS sustainable_campaign_pct
FROM vw_growth_strategy;


-- Budget Resilience Score
SELECT
    ROUND(
        (SUM(revenue) / SUM(campaign_budget)) /
        STDDEV_POP(campaign_budget), 2
    ) AS cost_resilience_score
FROM vw_growth_strategy;

-- Revenue Volatility
SELECT
    ROUND(
        STDDEV_POP(revenue) / AVG(revenue), 2
    ) AS growth_volatility_index
FROM vw_growth_strategy;

-- Long-Term Growth Readiness Score (Composite KPI)
SELECT
    ROUND(
        (SUM(revenue)/SUM(campaign_budget)) *
        (SUM(conversions)/SUM(clicks)) *
        (AVG(total_engagement)/100),
    2) AS growth_readiness_score
FROM vw_growth_strategy;

-- text kpi
-- Growth Outlook Signal
SELECT
CASE
    WHEN (SUM(revenue)/SUM(campaign_budget)) > 3
         AND (SUM(conversions)/SUM(clicks)) > 0.15
    THEN 'HIGHLY SCALABLE – STRONG UNIT ECONOMICS & EXECUTION'

    WHEN (SUM(conversions)/SUM(clicks)) < 0.08
    THEN 'FUNNEL LEAKAGE RISK – TARGETED OPTIMIZATION REQUIRED'

    ELSE 'STABLE GROWTH – DISCIPLINED EXECUTION ADVISED'
END AS executive_growth_risk_outlook
FROM vw_growth_strategy;

