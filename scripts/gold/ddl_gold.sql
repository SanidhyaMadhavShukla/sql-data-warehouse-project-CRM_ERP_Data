/*
======================================================================================
Create View for Gold Layer (DDL Script)
======================================================================================
Script Purpose:
    This script is used to create or update views as gold layer schema objects. These Objects
    Store data that has been transformed according to business specifications
*/
-- Create/Update View containing the dimension customers
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
	cci.cst_id as customer_id,
	cci.cst_key as customer_number,
	cci.cst_firstname as firstname,
	cci.cst_lastname as lastname,
	cci.cst_marital_status as maritial_status,
	CASE
		WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr -- crm is main/master system for gender data 
		ELSE COALESCE(eca.GEN, 'n/a')
	END as gender,
	eca.BDATE as birthday,
	ela.CNTRY as country,
	cci.cst_create_date as create_date
FROM 
	silver.crm_cust_info as cci
	LEFT JOIN silver.erp_cust_az12 as eca ON cci.cst_key = eca.CID
	LEFT JOIN silver.erp_loc_a101 as ela ON cci.cst_key = ela.CID;

GO
-- Create/Update View containing the dimension products
CREATE OR ALTER VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cpi.prd_key,cpi.prd_start_dt) as product_key,
	cpi.prd_id as product_id,
	cpi.prd_key as product_number,
	cpi.prd_nm as product_name,
	cpi.prd_cat_id as category_id,
	epcg.CAT as category,
	epcg.SUBCAT as subcategory,
	cpi.prd_line as product_line,
	epcg.MAINTENANCE as maintenance,
	cpi.prd_cost as cost,
	cpi.prd_start_dt as start_date
FROM 
	silver.crm_prd_info as cpi
	LEFT JOIN silver.erp_px_cat_g1v2 as epcg ON cpi.prd_cat_id = epcg.ID
WHERE
	cpi.prd_end_dt IS NULL;
GO
-- Create/Update View containing the fact sales
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
	csd.sls_ord_num as order_number,
	dp.product_key,
	dc.customer_key,
	csd.sls_order_dt as order_date,
	csd.sls_ship_dt as shipping_date,
	csd.sls_due_dt as due_date,
	csd.sls_quantity as quantity,
	csd.sls_price as price,
	csd.sls_sales as sale_amount
FROM 
	silver.crm_sales_details as csd
	LEFT JOIN gold.dim_products as dp ON dp.product_number = csd.sls_prd_key
	LEFT JOIN gold.dim_customers as dc ON dc.customer_id = csd.sls_cust_id;
