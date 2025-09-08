-- OLTP Query 1: Customer Information Lookup
-- Purpose: Retrieve complete customer profile for customer service
-- Usage: Single customer lookup for support tickets, account updates

SELECT 
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    c.c_email_address,
    c.c_birth_country,
    c.c_birth_year,
    ca.ca_street_number,
    ca.ca_street_name,
    ca.ca_city,
    ca.ca_state,
    ca.ca_zip,
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
WHERE c.c_customer_id = 'AAAAAAAABGHKNJAA';
