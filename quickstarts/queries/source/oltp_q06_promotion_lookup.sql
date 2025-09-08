-- OLTP Query 6: Active Promotions Check
-- Purpose: Check for active promotions for specific items or stores
-- Usage: Point of sale systems, e-commerce discount application

SELECT 
    p.p_promo_id,
    p.p_promo_name,
    p.p_purpose,
    start_d.d_date AS start_date,
    end_d.d_date AS end_date,
    p.p_cost AS promo_cost,
    p.p_response_target,
    CASE 
        WHEN start_d.d_date <= CURRENT_DATE AND end_d.d_date >= CURRENT_DATE THEN 'Active'
        WHEN start_d.d_date > CURRENT_DATE THEN 'Upcoming'
        ELSE 'Expired'
    END AS promo_status,
    DATEDIFF(end_d.d_date, CURRENT_DATE) AS days_remaining
FROM promotion p
JOIN date_dim start_d ON p.p_start_date_sk = start_d.d_date_sk
JOIN date_dim end_d ON p.p_end_date_sk = end_d.d_date_sk
WHERE (start_d.d_date <= CURRENT_DATE AND end_d.d_date >= CURRENT_DATE)
    OR (start_d.d_date > CURRENT_DATE AND start_d.d_date <= CURRENT_DATE + INTERVAL '7 days')
ORDER BY start_d.d_date, p.p_promo_name;
