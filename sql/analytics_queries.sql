-- Analytical queries for the Helix Finance mini warehouse.
-- Each named block is parsed by finance_dwh/analytics.py and rendered in the
-- Streamlit "Analytics" tab. All queries target the star schema created by
-- python -m finance_dwh.pipeline.

-- name: portfolio_by_product
-- title: Portfolio by product
-- description: Loan counts, principal, active exposure, and average rate by product.
-- chart: bar
SELECT
    p.product_name,
    COUNT(*) AS loan_count,
    ROUND(SUM(f.principal_amount), 2) AS total_principal,
    ROUND(SUM(CASE WHEN f.loan_status = 'active' THEN f.principal_amount ELSE 0 END), 2) AS active_principal,
    ROUND(AVG(f.interest_rate), 2) AS avg_interest_rate_pct
FROM fact_loan_originations AS f
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_principal DESC;

-- name: monthly_originations
-- title: Monthly origination trend
-- description: Originated principal and loan count by reporting month.
-- chart: line
SELECT
    d.year_month AS month,
    COUNT(*) AS loan_count,
    ROUND(SUM(f.principal_amount), 2) AS originated_principal,
    ROUND(AVG(f.interest_rate), 2) AS avg_interest_rate_pct
FROM fact_loan_originations AS f
JOIN dim_date AS d ON d.date_key = f.origination_date_key
GROUP BY d.year_month
ORDER BY d.year_month;

-- name: transaction_activity
-- title: Transaction activity and success rate
-- description: Monthly transaction volume, payment volume, failures, and success rate.
-- chart: line
SELECT
    d.year_month AS month,
    COUNT(*) AS transaction_count,
    ROUND(SUM(CASE WHEN f.is_successful THEN f.amount ELSE 0 END), 2) AS successful_volume,
    SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_successful THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
FROM fact_transactions AS f
JOIN dim_date AS d ON d.date_key = f.transaction_date_key
GROUP BY d.year_month
ORDER BY d.year_month;

-- name: top_customers
-- title: Top customers by payment volume
-- description: Top 10 customers ranked by successful payment volume.
-- chart: table
SELECT
    c.customer_id,
    c.full_name AS customer,
    c.segment,
    c.country,
    COUNT(*) AS successful_payments,
    ROUND(SUM(f.amount), 2) AS payment_volume
FROM fact_transactions AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
WHERE f.is_successful = TRUE
  AND f.transaction_type = 'payment'
GROUP BY c.customer_id, c.full_name, c.segment, c.country
ORDER BY payment_volume DESC
LIMIT 10;

-- name: loan_status_summary
-- title: Loan book by status
-- description: Exposure and share of loans across servicing statuses.
-- chart: bar
SELECT
    f.loan_status,
    COUNT(*) AS loan_count,
    ROUND(SUM(f.principal_amount), 2) AS principal,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_loans
FROM fact_loan_originations AS f
GROUP BY f.loan_status
ORDER BY principal DESC;

-- name: segment_performance
-- title: Customer segment performance
-- description: Loan penetration and average loan size by customer segment.
-- chart: bar
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_key) AS customers,
    COUNT(f.loan_key) AS loans,
    ROUND(COALESCE(SUM(f.principal_amount), 0), 2) AS total_principal,
    ROUND(COALESCE(AVG(f.principal_amount), 0), 2) AS avg_loan_size,
    ROUND(COALESCE(AVG(f.interest_rate), 0), 2) AS avg_interest_rate_pct
FROM dim_customer AS c
LEFT JOIN fact_loan_originations AS f ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY total_principal DESC;
