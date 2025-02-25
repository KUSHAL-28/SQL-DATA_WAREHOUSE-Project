SELECT 
CASE
	WHEN sls_order_dt=0 or LEN(sls_order_dt)!=8
	THEN NULL
	ELSE CAST(CAST(sls_order_dt as nvarchar) as date)
END sls_order_dt
FROM
bronze.crm_sales_details
WHERE sls_order_dt is NULL or LEN(sls_order_dt)<8

SELECT
sls_sales,
sls_quantity,
sls_price
FROM
bronze.crm_sales_details
WHERE sls_sales!=sls_quantity*sls_price
OR sls_quantity is NULL 
OR sls_price is NULL
--If sales is negative zero or null derive it using quantity and price. 
--If price is zero or null, calculate it using sales and quantity. 
--If price is negative, convert it into a positive value.

--Check Invalid DATE orders
SELECT
*
FROM
bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR sls_order_dt>sls_due_dt
