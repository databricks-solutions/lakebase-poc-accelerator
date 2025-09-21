-- PARAMETERS: [["N"], ["Y"]]
-- EXEC_COUNT: 1

SELECT * FROM customer where c_preferred_cust_flag = %s limit 1000;
