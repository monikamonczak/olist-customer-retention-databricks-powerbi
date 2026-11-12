-- =============================================================================
-- Olist Customer Retention: validation queries (Databricks SQL / Spark SQL)
-- Every figure used in the README and the dashboard can be reproduced here.
-- Run each query in its own cell: Databricks shows only the last result of a cell.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- A. Silver layer
-- ---------------------------------------------------------------------------

-- A1. Orders by status (expected total 99,433: delivered 96,470; shipped 1,107; canceled 625;
--     unavailable 609; invoiced 314; processing 301; created 5; approved 2)
SELECT order_status, COUNT(*) AS orders
FROM olist_project.silver.orders
GROUP BY order_status
ORDER BY orders DESC;

-- A2. Order items of orders removed in Silver (delivered without a delivery date).
--     Expected: 8 items, price 1,249.18
SELECT COUNT(*) AS items_of_removed_orders,
       ROUND(SUM(i.price), 2) AS price_of_removed_orders
FROM olist_project.silver.order_items i
LEFT JOIN olist_project.silver.orders o ON i.order_id = o.order_id
WHERE o.order_id IS NULL;

-- A3. Customers whose only orders were removed in Silver (expected: 8)
SELECT COUNT(*) AS customers_without_orders FROM (
    SELECT DISTINCT customer_unique_id FROM olist_project.silver.customers
    EXCEPT
    SELECT DISTINCT c.customer_unique_id
    FROM olist_project.silver.customers c
    JOIN olist_project.silver.orders o ON c.customer_id = o.customer_id
);

-- A4. Orders without any item (expected: 775; they are lost by an inner join to order_items)
SELECT COUNT(*) AS orders_without_items
FROM olist_project.silver.orders o
LEFT JOIN olist_project.silver.order_items i ON o.order_id = i.order_id
WHERE i.order_id IS NULL;

-- ---------------------------------------------------------------------------
-- B. Gold: fact_orders (one row per order)
-- ---------------------------------------------------------------------------

-- B1. Grain: rows must equal distinct orders (expected: 99,433 and 99,433)
SELECT COUNT(*) AS fact_rows, COUNT(DISTINCT order_id) AS orders
FROM olist_project.gold.fact_orders;

-- B2. Customers kept (expected: 96,088 = 96,096 in Silver minus the 8 from A3)
SELECT COUNT(DISTINCT customer_unique_id) AS customers
FROM olist_project.gold.fact_orders;

-- B3. Revenue is not duplicated: both values expected 13,590,394.52
SELECT ROUND(SUM(items_value), 2) AS revenue_in_fact FROM olist_project.gold.fact_orders;

SELECT ROUND(SUM(i.price), 2) AS revenue_in_items
FROM olist_project.silver.order_items i
JOIN olist_project.silver.orders o ON i.order_id = o.order_id;

-- B4. Late flag is consistent (expected: 0, 0, 6,534 late orders, 96,470 delivered orders)
SELECT SUM(CASE WHEN delay_days = 0 AND is_late = 1 THEN 1 ELSE 0 END) AS same_day_flagged_late,
       SUM(CASE WHEN is_delivered = 0 AND is_late IS NOT NULL THEN 1 ELSE 0 END) AS undelivered_with_flag,
       SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) AS late_orders,
       SUM(is_delivered) AS delivered_orders
FROM olist_project.gold.fact_orders;

-- B5. Revenue by order status (expected: delivered 13,220,248.93 = 97.3%; other statuses 370,145.59)
SELECT o.order_status,
       COUNT(DISTINCT o.order_id) AS orders,
       ROUND(SUM(i.price), 2) AS revenue
FROM olist_project.silver.orders o
JOIN olist_project.silver.order_items i ON o.order_id = i.order_id
GROUP BY o.order_status
ORDER BY revenue DESC;

-- B6. Range of delivered purchases (expected: 2016-09-15 to 2018-08-29)
SELECT MIN(purchase_date) AS first_purchase, MAX(purchase_date) AS last_purchase
FROM olist_project.gold.fact_orders
WHERE is_delivered = 1;

-- ---------------------------------------------------------------------------
-- C. Gold: dim_customers (one row per customer with a delivered order)
-- ---------------------------------------------------------------------------

-- C1. Counts (expected: 93,350 customers; 2,015 returning; 56,034 observable for 180 days; 1,109 returned)
SELECT COUNT(*) AS customers,
       SUM(CASE WHEN customer_segment = 'Returning Customer' THEN 1 ELSE 0 END) AS returning_customers,
       SUM(is_observable_180d) AS observable_180d,
       SUM(returned_within_180d) AS returned_within_180d
FROM olist_project.gold.dim_customers;

-- C2. Customers with 2+ delivered orders, and those who never bought on a later day
--     (expected: 2,801 customers with 2+ orders; 786 of them bought only on one day)
SELECT SUM(CASE WHEN total_orders >= 2 THEN 1 ELSE 0 END) AS customers_with_2plus_orders,
       SUM(CASE WHEN total_orders >= 2 AND purchase_days = 1 THEN 1 ELSE 0 END) AS all_orders_on_one_day
FROM olist_project.gold.dim_customers;

-- C3. Days from the first purchase to the first purchase on a later day
--     (expected: 2,015 returners; p25 23, median 74, p75 175, p90 288; right-censored)
SELECT COUNT(*) AS returners,
       percentile_approx(days_to_second_purchase, 0.25) AS p25,
       percentile_approx(days_to_second_purchase, 0.50) AS median,
       percentile_approx(days_to_second_purchase, 0.75) AS p75,
       percentile_approx(days_to_second_purchase, 0.90) AS p90
FROM olist_project.gold.dim_customers
WHERE second_purchase_date IS NOT NULL;

-- ---------------------------------------------------------------------------
-- D. Gold: rfm_segments
-- ---------------------------------------------------------------------------

-- D1. Segments (expected customers / revenue):
--     Lost 54,440 / 7,484,089.62; At risk 19,059 / 2,722,688.79; New 17,836 / 2,466,833.97;
--     Loyal - active 1,076 / 295,866.60; Loyal - lapsed 939 / 250,769.95
--     Totals: 93,350 customers; 13,220,248.93 revenue
SELECT segment,
       COUNT(*) AS customers,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
       ROUND(SUM(monetary), 2) AS revenue,
       ROUND(100.0 * SUM(monetary) / SUM(SUM(monetary)) OVER (), 2) AS pct_of_revenue,
       ROUND(AVG(monetary), 0) AS avg_spend
FROM olist_project.gold.rfm_segments
GROUP BY segment
ORDER BY customers DESC;

-- ---------------------------------------------------------------------------
-- E. Findings
-- ---------------------------------------------------------------------------

-- E1. 180-day return rate by delivery of the FIRST order
--     (expected: on time 52,215 customers / 1,048 returned / 2.01%; late 3,819 / 61 / 1.60%)
SELECT first_order_is_late,
       COUNT(*) AS customers,
       SUM(returned_within_180d) AS returned,
       ROUND(100.0 * SUM(returned_within_180d) / COUNT(*), 2) AS return_rate_pct
FROM olist_project.gold.dim_customers
WHERE is_observable_180d = 1
GROUP BY first_order_is_late;

-- E2. Two-proportion z-test for E1 (expected: diff about 0.0041, z about 1.76, i.e. p about 0.08)
WITH g AS (
  SELECT first_order_is_late AS grp, COUNT(*) AS n, SUM(returned_within_180d) AS x
  FROM olist_project.gold.dim_customers
  WHERE is_observable_180d = 1
  GROUP BY first_order_is_late
),
a AS (SELECT n AS n0, x AS x0 FROM g WHERE grp = 0),
b AS (SELECT n AS n1, x AS x1 FROM g WHERE grp = 1)
SELECT x0 / n0 AS rate_on_time,
       x1 / n1 AS rate_late,
       x0 / n0 - x1 / n1 AS diff,
       (x0 / n0 - x1 / n1)
         / SQRT(((x0 + x1) / (n0 + n1)) * (1 - (x0 + x1) / (n0 + n1)) * (1.0 / n0 + 1.0 / n1)) AS z
FROM a CROSS JOIN b;

-- E3. 180-day return rate by review score of the FIRST order
--     (expected customers / returned: 1: 5,610 / 96; 2: 1,781 / 28; 3: 4,861 / 94;
--      4: 11,157 / 205; 5: 32,208 / 681; No review: 417 / 5)
SELECT CASE WHEN first_order_review_score IS NULL THEN 'No review'
            ELSE CAST(ROUND(first_order_review_score, 0) AS STRING) END AS score,
       COUNT(*) AS customers,
       SUM(returned_within_180d) AS returned,
       ROUND(100.0 * SUM(returned_within_180d) / COUNT(*), 2) AS return_rate_pct
FROM olist_project.gold.dim_customers
WHERE is_observable_180d = 1
GROUP BY 1
ORDER BY 1;

-- E4. Late deliveries by customer state
--     (expected: AL 21.4%, MA 17.4%, SE 15.2%, PI 13.9%, CE 13.8% ... SP 40,494 orders / 4.5%;
--      SP + RJ hold 3,315 of the 6,534 late orders)
SELECT customer_state,
       COUNT(*) AS delivered_orders,
       SUM(is_late) AS late_orders,
       ROUND(100.0 * SUM(is_late) / COUNT(*), 1) AS late_pct
FROM olist_project.gold.fact_orders
WHERE is_delivered = 1
GROUP BY customer_state
ORDER BY late_pct DESC;

-- E5. Value of a returning customer: revenue from orders placed on LATER days
--     (expected: 2,015 returners; 2,260 later-day orders; revenue 284,625.92;
--      141.25 per returner; later-day AOV 125.94)
SELECT COUNT(DISTINCT o.customer_unique_id) AS returners,
       COUNT(*) AS later_day_orders,
       ROUND(SUM(o.items_value), 2) AS later_day_revenue,
       ROUND(SUM(o.items_value) / COUNT(DISTINCT o.customer_unique_id), 2) AS revenue_per_returner,
       ROUND(SUM(o.items_value) / COUNT(*), 2) AS later_day_aov
FROM olist_project.gold.fact_orders o
JOIN olist_project.gold.dim_customers c ON o.customer_unique_id = c.customer_unique_id
WHERE o.is_delivered = 1
  AND o.purchase_date > c.first_purchase_date;
