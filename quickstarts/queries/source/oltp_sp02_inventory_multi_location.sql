-- OLTP Stored Procedure 2: Multi-Location Inventory Check
-- Purpose: Check inventory across multiple warehouses for fulfillment
-- Usage: Order processing, inventory allocation, supply chain

CREATE OR REPLACE PROCEDURE sp_inventory_multi_location_check(
    IN p_item_id VARCHAR(100),
    IN p_required_quantity INTEGER,
    OUT p_availability_cursor REFCURSOR,
    OUT p_total_available INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_stock INTEGER := 0;
BEGIN
    -- Calculate total available stock
    SELECT COALESCE(SUM(inv.inv_quantity_on_hand), 0)
    INTO v_total_stock
    FROM inventory inv
    JOIN item i ON inv.inv_item_sk = i.i_item_sk
    JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk
    WHERE i.i_item_id = p_item_id
        AND d.d_date = CURRENT_DATE
        AND inv.inv_quantity_on_hand > 0;
    
    -- Set output parameter
    p_total_available := v_total_stock;
    
    -- Open cursor for detailed warehouse breakdown
    OPEN p_availability_cursor FOR
        SELECT 
            w.w_warehouse_sk,
            w.w_warehouse_name,
            w.w_city,
            w.w_state,
            w.w_country,
            i.i_item_id,
            i.i_item_desc,
            i.i_current_price,
            inv.inv_quantity_on_hand AS available_stock,
            CASE 
                WHEN inv.inv_quantity_on_hand >= p_required_quantity THEN 'Sufficient'
                WHEN inv.inv_quantity_on_hand > 0 THEN 'Partial'
                ELSE 'Out of Stock'
            END AS stock_status,
            CASE 
                WHEN inv.inv_quantity_on_hand >= p_required_quantity THEN p_required_quantity
                ELSE inv.inv_quantity_on_hand
            END AS can_fulfill,
            d.d_date AS inventory_date
        FROM inventory inv
        JOIN item i ON inv.inv_item_sk = i.i_item_sk
        JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
        JOIN date_dim d ON inv.inv_date_sk = d.d_date_sk
        WHERE i.i_item_id = p_item_id
            AND d.d_date = CURRENT_DATE
        ORDER BY inv.inv_quantity_on_hand DESC, w.w_warehouse_name;
        
END;
$$;
