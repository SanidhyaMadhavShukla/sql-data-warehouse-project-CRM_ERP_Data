/*
=======================================================================
Quality Checks
=======================================================================
Script Purpose:
		This script contains all the tests done on the  data while creating and validating
		gold layer.
*/
--Checking for Duplicates in Table
Select cst_id, count(*) as number_of_records
from (
	SELECT 
		cci.cst_id,
		cci.cst_key,
		cci.cst_firstname,
		cci.cst_lastname,
		cci.cst_marital_status,
		cci.cst_gndr,
		cci.cst_create_date,
		eca.BDATE,
		eca.GEN,
		ela.CNTRY
	FROM 
		silver.crm_cust_info as cci
		LEFT JOIN silver.erp_cust_az12 as eca ON cci.cst_key = eca.CID
		LEFT JOIN silver.erp_loc_a101 as ela ON cci.cst_key = ela.CID
) as Temp
Group by cst_id
Having count(*) > 1;

select DISTINCT gender from gold.dim_customers;

select prd_key, COUNT(1)
from (
	SELECT 
		cpi.prd_id,
		cpi.prd_cat_id,
		cpi.prd_key,
		cpi.prd_nm,
		cpi.prd_cost,
		cpi.prd_line,
		cpi.prd_start_dt,
		epcg.CAT,
		epcg.SUBCAT,
		epcg.MAINTENANCE
	FROM 
		silver.crm_prd_info as cpi
		LEFT JOIN silver.erp_px_cat_g1v2 as epcg ON cpi.prd_cat_id = epcg.ID
	WHERE
		cpi.prd_end_dt IS NULL
) as Temp
GROUP BY prd_key
HAVING COUNT(1) > 1;

--Checking for Distinct Values in similar columns in Tables
Select *
from (
	SELECT DISTINCT
		cci.cst_gndr,
		eca.GEN,
		CASE
		WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
		ELSE COALESCE(eca.GEN, 'n/a')
	END as new_gender
	FROM 
		silver.crm_cust_info as cci
		LEFT JOIN silver.erp_cust_az12 as eca ON cci.cst_key = eca.CID
		LEFT JOIN silver.erp_loc_a101 as ela ON cci.cst_key = ela.CID
) as Temp;

--Check Foreign Key Integrity
select *
FROM 
	gold.fact_sales as fs
	LEFT JOIN gold.dim_products as dp ON dp.product_key = fs.product_key 
	LEFT JOIN gold.dim_customers as dc ON dc.customer_key = fs.customer_key
WHERE
	dc.customer_key IS NULL OR

	dp.product_key IS NULL;
