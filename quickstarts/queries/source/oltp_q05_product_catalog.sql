-- OLTP Query 5: Product Catalog Search
-- Purpose: Search products by category for e-commerce browsing
-- Usage: Website product listing, mobile app category browsing

SELECT 
    i.i_item_id,
    i.i_item_desc,
    i.i_brand,
    i.i_class,
    i.i_category,
    i.i_current_price,
    i.i_size,
    i.i_color,
    i.i_units,
    CASE 
        WHEN i.i_current_price < 10 THEN 'Budget'
        WHEN i.i_current_price < 50 THEN 'Standard'
        WHEN i.i_current_price < 100 THEN 'Premium'
        ELSE 'Luxury'
    END AS price_range
FROM item i
WHERE i.i_category = 'CATEGORY_PLACEHOLDER'
    AND i.i_current_price IS NOT NULL
    AND i.i_current_price > 0
ORDER BY i.i_brand, i.i_current_price
LIMIT 50;
