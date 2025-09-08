-- OLTP Query 3: Real-time Inventory Check
-- Purpose: Check current inventory levels for specific item at warehouse
-- Usage: Before order processing, stock availability checks

SELECT 
    i.i_item_id,
    i.i_item_desc,
    i.i_brand,
    i.i_category,
    i.i_current_price,
    w.w_warehouse_name,
    w.w_city AS warehouse_city,
    w.w_state AS warehouse_state,
    inv.inv_quantity_on_hand AS current_stock,
    CASE 
        WHEN inv.inv_quantity_on_hand > 100 THEN 'In Stock'
        WHEN inv.inv_quantity_on_hand > 10 THEN 'Low Stock'
        WHEN inv.inv_quantity_on_hand > 0 THEN 'Very Low Stock'
        ELSE 'Out of Stock'
    END AS stock_status
FROM inventory inv
JOIN item i ON inv.inv_item_sk = i.i_item_sk
JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk
WHERE i.i_item_id = 'ITEM_ID_PLACEHOLDER'
    AND w.w_warehouse_sk = WAREHOUSE_SK_PLACEHOLDER
    AND d.d_date = CURRENT_DATE;
