-- OLTP Query 4: Customer Recent Purchase History
-- Purpose: Show customer's recent purchase history for personalization
-- Usage: Customer service, recommendation engine, account review

SELECT 
    ss.ss_ticket_number,
    d.d_date AS purchase_date,
    s.s_store_name,
    s.s_city AS store_city,
    COUNT(ss.ss_item_sk) AS items_purchased,
    SUM(ss.ss_quantity) AS total_quantity,
    SUM(ss.ss_net_paid) AS total_amount,
    AVG(ss.ss_sales_price) AS avg_item_price
FROM store_sales ss
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE c.c_customer_id = 'CUSTOMER_ID_PLACEHOLDER'
    AND d.d_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY ss.ss_ticket_number, d.d_date, s.s_store_name, s.s_city
ORDER BY d.d_date DESC
LIMIT 10;
