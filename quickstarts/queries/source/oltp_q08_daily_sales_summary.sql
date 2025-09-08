-- OLTP Query 8: Daily Sales Summary for Store
-- Purpose: Quick daily sales summary for store managers
-- Usage: End of day reporting, manager dashboard

SELECT 
    s.s_store_name,
    s.s_store_id,
    d.d_date AS sales_date,
    d.d_day_name,
    COUNT(DISTINCT ss.ss_ticket_number) AS total_transactions,
    COUNT(ss.ss_item_sk) AS total_items_sold,
    SUM(ss.ss_quantity) AS total_quantity,
    SUM(ss.ss_sales_price) AS gross_sales,
    SUM(ss.ss_coupon_amt) AS total_discounts,
    SUM(ss.ss_net_paid) AS net_sales,
    AVG(ss.ss_sales_price) AS avg_item_price,
    MAX(ss.ss_sales_price) AS highest_sale_item,
    MIN(ss.ss_sales_price) AS lowest_sale_item
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE s.s_store_id = 'STORE_ID_PLACEHOLDER'
    AND d.d_date = 'SALES_DATE_PLACEHOLDER'
GROUP BY s.s_store_name, s.s_store_id, d.d_date, d.d_day_name;
