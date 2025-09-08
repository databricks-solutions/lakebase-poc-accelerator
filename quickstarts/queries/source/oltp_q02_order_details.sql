-- OLTP Query 2: Order Details Lookup
-- Purpose: Retrieve complete order information for order tracking
-- Usage: Customer service order inquiries, order status checks

SELECT 
    ss.ss_ticket_number AS order_number,
    ss.ss_sold_date_sk,
    d.d_date AS order_date,
    i.i_item_id,
    i.i_item_desc,
    i.i_brand,
    i.i_category,
    ss.ss_quantity,
    ss.ss_list_price,
    ss.ss_sales_price,
    ss.ss_coupon_amt,
    ss.ss_net_paid,
    s.s_store_name,
    s.s_city AS store_city,
    s.s_state AS store_state
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE ss.ss_ticket_number = '239760001'
ORDER BY i.i_item_id;
