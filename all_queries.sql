-- ============================================================
-- PL Assignment — SQL Queries
-- Database: pl_assyment
-- Tables: contracts, contract_sign_up_details, billing_histories
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. SUBSCRIPTION OVERVIEW
-- ────────────────────────────────────────────────────────────

-- 1.1 Total subscriptions
SELECT COUNT(*) AS total_subscriptions
FROM contracts;

-- 1.2 Subscriptions per product
SELECT
    product_identifier,
    COUNT(*) AS subscriptions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM contracts
GROUP BY product_identifier
ORDER BY subscriptions DESC;

-- 1.3 Monthly signup trend
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS signup_month,
    COUNT(*) AS new_subscriptions
FROM contracts
GROUP BY signup_month
ORDER BY signup_month;

-- 1.4 Subscriptions by state
SELECT
    state,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM contracts
GROUP BY state
ORDER BY count DESC;

-- 1.5 Subscriptions by OS family
SELECT
    s.os_family,
    COUNT(*) AS subscriptions
FROM contract_sign_up_details s
GROUP BY s.os_family
ORDER BY subscriptions DESC;

-- 1.6 Subscriptions by campaign
SELECT
    s.campaign_id,
    s.campaign_name,
    COUNT(*) AS subscriptions
FROM contract_sign_up_details s
GROUP BY s.campaign_id, s.campaign_name
ORDER BY subscriptions DESC
LIMIT 20;

-- 1.7 Subscriptions by publisher (placement)
SELECT
    s.publisher_id,
    COUNT(*) AS subscriptions
FROM contract_sign_up_details s
GROUP BY s.publisher_id
ORDER BY subscriptions DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 2. REVENUE ANALYSIS
-- ────────────────────────────────────────────────────────────

-- 2.1 Total revenue from successful billings
SELECT
    ROUND(SUM(amount_in_euro_cents) / 100.0, 2) AS total_revenue_eur,
    COUNT(*) AS successful_billing_count
FROM billing_histories
WHERE status = 'ok';

-- 2.2 Revenue per product
SELECT
    product_identifier,
    ROUND(SUM(amount_in_euro_cents) / 100.0, 2) AS revenue_eur,
    COUNT(DISTINCT contract_id) AS paying_subscriptions,
    ROUND(SUM(amount_in_euro_cents) / 100.0 / COUNT(DISTINCT contract_id), 2) AS avg_revenue_per_sub
FROM billing_histories
WHERE status = 'ok'
GROUP BY product_identifier
ORDER BY revenue_eur DESC;

-- 2.3 Monthly revenue trend
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS billing_month,
    ROUND(SUM(amount_in_euro_cents) / 100.0, 2) AS revenue_eur,
    COUNT(*) AS successful_billings
FROM billing_histories
WHERE status = 'ok'
GROUP BY billing_month
ORDER BY billing_month;

-- 2.4 Average revenue per subscription
SELECT
    ROUND(AVG(sub_rev) / 100.0, 2) AS avg_revenue_per_sub_eur
FROM (
    SELECT contract_id, SUM(amount_in_euro_cents) AS sub_rev
    FROM billing_histories
    WHERE status = 'ok'
    GROUP BY contract_id
) t;

-- 2.5 Revenue by campaign
SELECT
    s.campaign_id,
    COUNT(DISTINCT s.contract_id) AS subscriptions,
    ROUND(SUM(b.amount_in_euro_cents) / 100.0, 2) AS total_revenue_eur,
    ROUND(SUM(b.amount_in_euro_cents) / 100.0 / COUNT(DISTINCT s.contract_id), 2) AS avg_revenue_per_sub
FROM contract_sign_up_details s
JOIN billing_histories b ON b.contract_id = s.contract_id AND b.status = 'ok'
GROUP BY s.campaign_id
ORDER BY total_revenue_eur DESC
LIMIT 20;

-- 2.6 Revenue by publisher
SELECT
    s.publisher_id,
    COUNT(DISTINCT s.contract_id) AS subscriptions,
    ROUND(SUM(b.amount_in_euro_cents) / 100.0, 2) AS total_revenue_eur,
    ROUND(SUM(b.amount_in_euro_cents) / 100.0 / COUNT(DISTINCT s.contract_id), 2) AS avg_revenue_per_sub
FROM contract_sign_up_details s
JOIN billing_histories b ON b.contract_id = s.contract_id AND b.status = 'ok'
GROUP BY s.publisher_id
ORDER BY total_revenue_eur DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 3. BILLING PERFORMANCE
-- ────────────────────────────────────────────────────────────

-- 3.1 Overall billing success rate
SELECT
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded,
    ROUND(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM billing_histories;

-- 3.2 Billing success rate per product
SELECT
    product_identifier,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS successful,
    ROUND(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM billing_histories
GROUP BY product_identifier
ORDER BY total_attempts DESC;

-- 3.3 Top billing failure reasons
SELECT
    reason,
    COUNT(*) AS occurrences,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM billing_histories
WHERE status = 'failed'
GROUP BY reason
ORDER BY occurrences DESC
LIMIT 15;

-- 3.4 Monthly billing success rate trend
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS billing_month,
    COUNT(*) AS total_attempts,
    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS successful,
    ROUND(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM billing_histories
GROUP BY billing_month
ORDER BY billing_month;

-- 3.5 Billing success rate by publisher (min 50 attempts)
SELECT
    s.publisher_id,
    COUNT(b.id) AS total_attempts,
    SUM(CASE WHEN b.status = 'ok' THEN 1 ELSE 0 END) AS successful,
    ROUND(SUM(CASE WHEN b.status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(b.id), 2) AS success_rate_pct
FROM contract_sign_up_details s
JOIN billing_histories b ON b.contract_id = s.contract_id
GROUP BY s.publisher_id
HAVING COUNT(b.id) >= 50
ORDER BY success_rate_pct DESC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 4. RETENTION & CHURN
-- ────────────────────────────────────────────────────────────

-- 4.1 Overall churn rate
SELECT
    COUNT(*) AS total_subscriptions,
    SUM(CASE WHEN terminated_at IS NOT NULL THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct,
    ROUND(AVG(CASE
        WHEN terminated_at IS NOT NULL
        THEN DATEDIFF(terminated_at, created_at)
        ELSE DATEDIFF('2025-01-31', created_at)
    END), 1) AS avg_lifetime_days
FROM contracts;

-- 4.2 Cohort retention (30 / 60 / 90 day)
SELECT
    DATE_FORMAT(created_at, '%Y-%m') AS cohort_month,
    COUNT(*) AS cohort_size,
    ROUND(SUM(CASE WHEN IFNULL(DATEDIFF(terminated_at, created_at), 999) >= 7  THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS day7_pct,
    ROUND(SUM(CASE WHEN IFNULL(DATEDIFF(terminated_at, created_at), 999) >= 14 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS day14_pct,
    ROUND(SUM(CASE WHEN IFNULL(DATEDIFF(terminated_at, created_at), 999) >= 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS day30_pct,
    ROUND(SUM(CASE WHEN IFNULL(DATEDIFF(terminated_at, created_at), 999) >= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS day60_pct,
    ROUND(SUM(CASE WHEN IFNULL(DATEDIFF(terminated_at, created_at), 999) >= 90 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS day90_pct
FROM contracts
GROUP BY cohort_month
ORDER BY cohort_month;

-- 4.3 Churn rate by campaign
SELECT
    s.campaign_id,
    COUNT(c.id) AS subscriptions,
    SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 2) AS churn_rate_pct,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
GROUP BY s.campaign_id
HAVING COUNT(c.id) >= 30
ORDER BY churn_rate_pct ASC;

-- 4.4 Churn by OS family
SELECT
    s.os_family,
    COUNT(c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 2) AS churn_rate_pct,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
GROUP BY s.os_family
ORDER BY subscriptions DESC;

-- 4.5 Churn by publisher (min 30 subs)
SELECT
    s.publisher_id,
    COUNT(c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 2) AS churn_rate_pct,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
GROUP BY s.publisher_id
HAVING COUNT(c.id) >= 30
ORDER BY churn_rate_pct ASC
LIMIT 20;


-- ────────────────────────────────────────────────────────────
-- 5. MARKETING EFFECTIVENESS
-- ────────────────────────────────────────────────────────────

-- 5.1 Full campaign scorecard
SELECT
    s.campaign_id,
    COUNT(DISTINCT c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 1) AS churn_rate_pct,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0, 2) AS total_revenue_eur,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0 / COUNT(DISTINCT c.id), 2) AS avg_revenue_per_sub
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
LEFT JOIN billing_histories b ON b.contract_id = c.id AND b.status = 'ok'
GROUP BY s.campaign_id
HAVING COUNT(DISTINCT c.id) >= 30
ORDER BY total_revenue_eur DESC;

-- 5.2 Publisher scorecard (top 20 by revenue)
SELECT
    s.publisher_id,
    COUNT(DISTINCT c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 1) AS churn_rate_pct,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0, 2) AS total_revenue_eur,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0 / COUNT(DISTINCT c.id), 2) AS avg_revenue_per_sub
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
LEFT JOIN billing_histories b ON b.contract_id = c.id AND b.status = 'ok'
GROUP BY s.publisher_id
HAVING COUNT(DISTINCT c.id) >= 30
ORDER BY total_revenue_eur DESC
LIMIT 20;

-- 5.3 OS family scorecard
SELECT
    s.os_family,
    COUNT(DISTINCT c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 1) AS churn_rate_pct,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0, 2) AS total_revenue_eur,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0 / COUNT(DISTINCT c.id), 2) AS avg_revenue_per_sub
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
LEFT JOIN billing_histories b ON b.contract_id = c.id AND b.status = 'ok'
GROUP BY s.os_family
ORDER BY subscriptions DESC;

-- 5.4 High-quality publishers: low churn + high revenue per sub
SELECT
    s.publisher_id,
    COUNT(DISTINCT c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 1) AS churn_rate_pct,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0 / COUNT(DISTINCT c.id), 2) AS avg_revenue_per_sub,
    ROUND(AVG(CASE WHEN c.terminated_at IS NOT NULL
        THEN DATEDIFF(c.terminated_at, c.created_at)
        ELSE DATEDIFF('2025-01-31', c.created_at)
    END), 1) AS avg_lifetime_days
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
LEFT JOIN billing_histories b ON b.contract_id = c.id AND b.status = 'ok'
GROUP BY s.publisher_id
HAVING COUNT(DISTINCT c.id) >= 50
    AND churn_rate_pct < 50
ORDER BY avg_revenue_per_sub DESC
LIMIT 20;

-- 5.5 Referrer host performance
SELECT
    s.referrer_host,
    COUNT(DISTINCT c.id) AS subscriptions,
    ROUND(SUM(CASE WHEN c.terminated_at IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(c.id), 1) AS churn_rate_pct,
    ROUND(IFNULL(SUM(b.amount_in_euro_cents), 0) / 100.0, 2) AS total_revenue_eur
FROM contracts c
JOIN contract_sign_up_details s ON s.contract_id = c.id
LEFT JOIN billing_histories b ON b.contract_id = c.id AND b.status = 'ok'
WHERE s.referrer_host IS NOT NULL
GROUP BY s.referrer_host
HAVING COUNT(DISTINCT c.id) >= 100
ORDER BY total_revenue_eur DESC
LIMIT 20;
