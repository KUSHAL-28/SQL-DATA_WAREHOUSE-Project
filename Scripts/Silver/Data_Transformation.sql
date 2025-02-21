-- 1.REMOVING UNWANTED SPACES
-- 2.DATA NORMALIZATION(making into meaningful values s to single m to married like that)
-- 3.HANDLING MISSING VALUES(al of them into n/a
-- 4.REMOVING DUPLICATES
-- checking null values and duplicates
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL

-- checking for whitespaces
SELECT cst_firstname
FROM 
bronze.crm_cust_info
WHERE cst_firstname!=TRIM(cst_firstname)

SELECT cst_lastname
FROM 
bronze.crm_cust_info
WHERE cst_lastname!=TRIM(cst_lastname)

-- checking null values and duplicates(cross verifying whether we have in silver layer or not)
SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL
