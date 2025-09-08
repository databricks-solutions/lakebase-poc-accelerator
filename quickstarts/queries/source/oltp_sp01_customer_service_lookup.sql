-- OLTP Stored Procedure 1: Customer Service Complete Lookup
-- Purpose: Comprehensive customer information for customer service agents
-- Usage: Customer service tickets, account support, returns processing

CREATE OR REPLACE PROCEDURE sp_customer_service_lookup(
    IN p_customer_id VARCHAR(100),
    OUT p_result_cursor REFCURSOR
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Open cursor for complete customer information
    OPEN p_result_cursor FOR
        SELECT 
            -- Customer basic info
            c.c_customer_id,
            c.c_first_name,
            c.c_last_name,
            c.c_email_address,
            c.c_birth_country,
            c.c_birth_year,
            
            -- Address information
            ca.ca_street_number,
            ca.ca_street_name,
            ca.ca_city,
            ca.ca_state,
            ca.ca_zip,
            ca.ca_country,
            
            -- Demographics
            cd.cd_gender,
            cd.cd_marital_status,
            cd.cd_education_status,
            cd.cd_credit_rating,
            cd.cd_purchase_estimate,
            
            -- Customer stats (last 30 days)
            customer_stats.recent_orders,
            customer_stats.recent_total_spent,
            customer_stats.avg_order_value,
            customer_stats.last_purchase_date
            
        FROM customer c
        JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
        LEFT JOIN (
            SELECT 
                ss.ss_customer_sk,
                COUNT(DISTINCT ss.ss_ticket_number) AS recent_orders,
                SUM(ss.ss_net_paid) AS recent_total_spent,
                AVG(ss.ss_net_paid) AS avg_order_value,
                MAX(d.d_date) AS last_purchase_date
            FROM store_sales ss
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
            WHERE d.d_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY ss.ss_customer_sk
        ) customer_stats ON c.c_customer_sk = customer_stats.ss_customer_sk
        WHERE c.c_customer_id = p_customer_id;
        
END;
$$;
