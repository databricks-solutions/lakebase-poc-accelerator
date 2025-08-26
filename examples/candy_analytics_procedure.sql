-- Stored Procedure: get_candy_analytics_multiple_results
-- Purpose: Analyze candy product data and return multiple result sets via INOUT cursors
-- Usage: CALL get_candy_analytics_multiple_results(NULL, NULL, NULL);
-- Returns: Three refcursors - overall stats, division breakdown, and factory breakdown

CREATE OR REPLACE PROCEDURE "default".get_candy_analytics_multiple_results(
    INOUT overall_cursor refcursor,
    INOUT division_cursor refcursor,
    INOUT factory_cursor refcursor
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Open cursor for overall statistics
    OPEN overall_cursor FOR
        SELECT 
            COUNT(*) as total_products,
            COUNT(DISTINCT "Division") as total_divisions,
            COUNT(DISTINCT "Factory") as total_factories,
            ROUND(CAST(AVG("Unit Price") AS numeric), 2) as avg_price,
            ROUND(CAST(AVG("Unit Cost") AS numeric), 2) as avg_cost,
            ROUND(CAST(AVG("Unit Price" - "Unit Cost") AS numeric), 2) as avg_profit,
            ROUND(CAST(MIN("Unit Price") AS numeric), 2) as min_price,
            ROUND(CAST(MAX("Unit Price") AS numeric), 2) as max_price,
            ROUND(CAST(SUM("Unit Price" - "Unit Cost") AS numeric), 2) as total_profit
        FROM "default".syncedcandy;
    
    -- Open cursor for division breakdown
    OPEN division_cursor FOR
        SELECT 
            "Division",
            COUNT(*) as product_count,
            ROUND(CAST(AVG("Unit Price") AS numeric), 2) as avg_price,
            ROUND(CAST(AVG("Unit Cost") AS numeric), 2) as avg_cost,
            ROUND(CAST(AVG("Unit Price" - "Unit Cost") AS numeric), 2) as avg_profit,
            ROUND(CAST(SUM("Unit Price" - "Unit Cost") AS numeric), 2) as total_profit,
            ROUND(CAST(MIN("Unit Price") AS numeric), 2) as min_price,
            ROUND(CAST(MAX("Unit Price") AS numeric), 2) as max_price
        FROM "default".syncedcandy
        GROUP BY "Division"
        ORDER BY "Division";
    
    -- Open cursor for factory breakdown
    OPEN factory_cursor FOR
        SELECT 
            "Factory",
            COUNT(*) as product_count,
            ROUND(CAST(AVG("Unit Price") AS numeric), 2) as avg_price,
            ROUND(CAST(AVG("Unit Cost") AS numeric), 2) as avg_cost,
            ROUND(CAST(AVG("Unit Price" - "Unit Cost") AS numeric), 2) as avg_profit,
            ROUND(CAST(SUM("Unit Price" - "Unit Cost") AS numeric), 2) as total_profit,
            ROUND(CAST(MIN("Unit Price") AS numeric), 2) as min_price,
            ROUND(CAST(MAX("Unit Price") AS numeric), 2) as max_price
        FROM "default".syncedcandy
        GROUP BY "Factory"
        ORDER BY "Factory";
        
EXCEPTION
    WHEN OTHERS THEN
        -- Log error and re-raise
        RAISE NOTICE 'Error in get_candy_analytics_multiple_results: %', SQLERRM;
        RAISE;
END;
$$;

-- Grant permissions (adjust as needed)
-- GRANT EXECUTE ON PROCEDURE get_candy_analytics_multiple_results(INOUT refcursor, INOUT refcursor, INOUT refcursor) TO your_role;