-- OLTP Query 7: Store Locator Service
-- Purpose: Find stores in specific geographic area
-- Usage: Store locator feature, delivery area checks

SELECT 
    s.s_store_sk,
    s.s_store_id,
    s.s_store_name,
    s.s_number_employees,
    s.s_floor_space,
    s.s_hours,
    s.s_manager,
    s.s_market_id,
    s.s_geography_class,
    s.s_market_desc,
    s.s_market_manager,
    s.s_division_id,
    s.s_division_name,
    s.s_company_id,
    s.s_company_name,
    s.s_street_number,
    s.s_street_name,
    s.s_street_type,
    s.s_suite_number,
    s.s_city,
    s.s_county,
    s.s_state,
    s.s_zip,
    s.s_country
FROM store s
WHERE s.s_state = 'STATE_PLACEHOLDER'
    AND s.s_city = 'CITY_PLACEHOLDER'
    AND s.s_store_name IS NOT NULL
ORDER BY s.s_store_name;
