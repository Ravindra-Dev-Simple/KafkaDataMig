-- ================================================================
-- Trino Test Queries for Banking Data Lakehouse
-- Run against: trino --server trino:8080 --catalog iceberg
-- ================================================================

-- ========================================
-- 1. BASIC CONNECTIVITY & TABLE DISCOVERY
-- ========================================

-- List all schemas
SHOW SCHEMAS FROM iceberg;

-- List tables in each layer
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- Table structure
DESCRIBE iceberg.silver.finacle_transactions;
DESCRIBE iceberg.silver.finacle_customers;
DESCRIBE iceberg.silver.lms_loans;

-- ========================================
-- 2. DATA VALIDATION QUERIES
-- ========================================

-- Row counts per layer (compare Bronze vs Silver — should match after dedup)
SELECT 'bronze.finacle_transactions' AS table_name, COUNT(*) AS row_count FROM iceberg.bronze.finacle_transactions
UNION ALL
SELECT 'silver.finacle_transactions', COUNT(*) FROM iceberg.silver.finacle_transactions
UNION ALL
SELECT 'bronze.finacle_customers', COUNT(*) FROM iceberg.bronze.finacle_customers
UNION ALL
SELECT 'silver.finacle_customers', COUNT(*) FROM iceberg.silver.finacle_customers
UNION ALL
SELECT 'bronze.lms_loans', COUNT(*) FROM iceberg.bronze.lms_loans
UNION ALL
SELECT 'silver.lms_loans', COUNT(*) FROM iceberg.silver.lms_loans;

-- ========================================
-- 3. BUSINESS ANALYTICS QUERIES
-- ========================================

-- Branch-wise transaction summary
SELECT
    branch_code,
    txn_type,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM iceberg.silver.finacle_transactions
GROUP BY branch_code, txn_type
ORDER BY total_amount DESC;

-- Channel distribution
SELECT
    channel,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM iceberg.silver.finacle_transactions
GROUP BY channel
ORDER BY txn_count DESC;

-- Hourly transaction pattern
SELECT
    txn_hour,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount
FROM iceberg.silver.finacle_transactions
GROUP BY txn_hour
ORDER BY txn_hour;

-- ========================================
-- 4. AML / FRAUD DETECTION QUERIES
-- ========================================

-- Flagged transactions detail
SELECT
    t.txn_id,
    t.customer_id,
    c.name AS customer_name,
    c.risk_category,
    c.kyc_status,
    t.amount,
    t.channel,
    t.narration,
    t.txn_date
FROM iceberg.silver.finacle_transactions t
JOIN iceberg.silver.finacle_customers c ON t.customer_id = c.customer_id
WHERE t.is_flagged = true
ORDER BY t.amount DESC;

-- Customers with multiple flagged transactions (structuring detection)
SELECT
    t.customer_id,
    c.name,
    c.risk_category,
    COUNT(*) AS flagged_count,
    SUM(t.amount) AS total_flagged_amount,
    ARRAY_AGG(t.txn_id) AS flagged_txn_ids
FROM iceberg.silver.finacle_transactions t
JOIN iceberg.silver.finacle_customers c ON t.customer_id = c.customer_id
WHERE t.is_flagged = true
GROUP BY t.customer_id, c.name, c.risk_category
HAVING COUNT(*) >= 2
ORDER BY flagged_count DESC;

-- High-value cash transactions (CTR threshold INR 10L)
SELECT
    t.txn_id,
    t.customer_id,
    c.name,
    t.amount,
    t.channel,
    t.narration,
    t.txn_date
FROM iceberg.silver.finacle_transactions t
JOIN iceberg.silver.finacle_customers c ON t.customer_id = c.customer_id
WHERE t.channel = 'CASH' AND t.amount >= 1000000
ORDER BY t.amount DESC;

-- ========================================
-- 5. NPA / REGULATORY QUERIES
-- ========================================

-- NPA Classification Summary (RBI format)
SELECT
    npa_status,
    COUNT(*) AS account_count,
    SUM(outstanding_principal) AS total_outstanding,
    SUM(sanctioned_amount) AS total_sanctioned,
    ROUND(AVG(interest_rate), 2) AS avg_rate,
    ROUND(AVG(dpd), 0) AS avg_dpd
FROM iceberg.silver.lms_loans
GROUP BY npa_status
ORDER BY
    CASE npa_status
        WHEN 'STANDARD' THEN 1
        WHEN 'SMA-1' THEN 2
        WHEN 'SMA-2' THEN 3
        WHEN 'NPA' THEN 4
    END;

-- Loans approaching NPA (SMA-1 and SMA-2)
SELECT
    l.loan_id,
    l.customer_id,
    l.customer_name,
    l.loan_type,
    l.outstanding_principal,
    l.dpd,
    l.npa_status,
    l.last_payment_date,
    l.next_due_date,
    l.collateral_type,
    l.collateral_value,
    l.ltv_ratio
FROM iceberg.silver.lms_loans l
WHERE l.npa_status IN ('SMA-1', 'SMA-2')
ORDER BY l.dpd DESC;

-- Provision requirement summary
SELECT
    npa_status,
    COUNT(*) AS accounts,
    SUM(outstanding_principal) AS total_outstanding,
    CASE npa_status
        WHEN 'STANDARD' THEN 0.40
        WHEN 'SMA-1' THEN 5.00
        WHEN 'SMA-2' THEN 15.00
        WHEN 'NPA' THEN 25.00
        ELSE 0
    END AS provision_pct,
    ROUND(SUM(outstanding_principal) *
        CASE npa_status
            WHEN 'STANDARD' THEN 0.004
            WHEN 'SMA-1' THEN 0.05
            WHEN 'SMA-2' THEN 0.15
            WHEN 'NPA' THEN 0.25
            ELSE 0
        END, 2) AS provision_amount
FROM iceberg.silver.lms_loans
GROUP BY npa_status;

-- ========================================
-- 6. CUSTOMER 360 QUERIES
-- ========================================

-- Customer 360 from Gold layer
SELECT * FROM iceberg.gold.customer_360
ORDER BY total_txn_amount DESC;

-- High-risk customers (cross-referencing transactions + loans + CIBIL)
SELECT
    c.customer_id,
    c.name,
    c.risk_category,
    c.kyc_status,
    cb.cibil_score,
    l.npa_status AS worst_loan_status,
    l.dpd AS max_dpd,
    l.outstanding_principal AS loan_outstanding,
    COUNT(DISTINCT CASE WHEN t.is_flagged THEN t.txn_id END) AS flagged_txns
FROM iceberg.silver.finacle_customers c
LEFT JOIN iceberg.bronze.cibil_bureau cb ON c.customer_id = cb.customer_id
LEFT JOIN iceberg.silver.lms_loans l ON c.customer_id = l.customer_id
LEFT JOIN iceberg.silver.finacle_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.name, c.risk_category, c.kyc_status,
         cb.cibil_score, l.npa_status, l.dpd, l.outstanding_principal
HAVING cb.cibil_score < 600 OR l.npa_status IN ('NPA', 'SMA-2') OR COUNT(DISTINCT CASE WHEN t.is_flagged THEN t.txn_id END) > 0
ORDER BY cb.cibil_score ASC;

-- ========================================
-- 7. ICEBERG-SPECIFIC QUERIES
-- ========================================

-- Time travel: read previous snapshot
SELECT * FROM iceberg.silver.finacle_transactions FOR VERSION AS OF 1;

-- Snapshot history
SELECT * FROM iceberg.silver."finacle_transactions$snapshots";

-- Partition info
SELECT * FROM iceberg.silver."finacle_transactions$partitions";

-- File manifest
SELECT * FROM iceberg.silver."finacle_transactions$files";

-- ========================================
-- 8. PERFORMANCE TEST QUERIES
-- ========================================

-- Explain plan for complex query
EXPLAIN ANALYZE
SELECT
    t.branch_code,
    t.channel,
    t.txn_type,
    COUNT(*) AS txn_count,
    SUM(t.amount) AS total_amount,
    COUNT(DISTINCT t.customer_id) AS customers
FROM iceberg.silver.finacle_transactions t
WHERE t.txn_month = '2026-04'
GROUP BY t.branch_code, t.channel, t.txn_type
ORDER BY total_amount DESC;

-- Partition pruning test (should only scan April partition)
EXPLAIN
SELECT COUNT(*), SUM(amount)
FROM iceberg.silver.finacle_transactions
WHERE txn_month = '2026-04';
