/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    The purpose of this script is to provide a list of Quality Checks to be performed on silver-layer data.
===============================================================================
*/
--Checking for Null and Duplicates in Primary Key

select cst_id, COUNT(1) as Duplicate_Count from silver.crm_cust_info GROUP BY cst_id HAVING COUNT(1) > 1 OR cst_id IS NULL;

select prd_id, COUNT(1) as Duplicate_Count from silver.crm_prd_info GROUP BY prd_id HAVING COUNT(1) > 1 OR prd_id IS NULL;

select sls_ord_num, sls_prd_key, COUNT(1) as Duplicate_Count from silver.crm_sales_details GROUP BY sls_ord_num, sls_prd_key HAVING COUNT(1) > 1 OR sls_ord_num IS NULL;

--Remove Unwanted Trailing Spaces
select cst_firstname,TRIM(cst_firstname) as Trimed_Name from silver.crm_cust_info where cst_firstname != TRIM(cst_firstname);
select cst_lastname,TRIM(cst_lastname) as Trimed_Name from silver.crm_cust_info where cst_lastname != TRIM(cst_lastname);

select prd_nm,TRIM(prd_nm) as Trimed_Name from silver.crm_prd_info where prd_nm != TRIM(prd_nm);

select sls_ord_num,TRIM(sls_ord_num) as Trimed_sls_ord_num from silver.crm_sales_details where sls_ord_num != TRIM(sls_ord_num);

select CAT,TRIM(CAT) as Trimed_CAT from silver.erp_px_cat_g1v2 where CAT != TRIM(CAT);
select SUBCAT,TRIM(SUBCAT) as Trimed_SUBCAT from silver.erp_px_cat_g1v2 where SUBCAT != TRIM(SUBCAT);
select MAINTENANCE,TRIM(MAINTENANCE) as Trimed_MAINTENANCE from silver.erp_px_cat_g1v2 where MAINTENANCE != TRIM(MAINTENANCE);

--Data Standardization and Consistency
select DISTINCT cst_marital_status from silver.crm_cust_info;
select DISTINCT cst_gndr from silver.crm_cust_info;

select DISTINCT prd_line from silver.crm_prd_info;

select DISTINCT GEN from silver.erp_cust_az12;

select DISTINCT CNTRY from silver.erp_loc_a101;

select DISTINCT MAINTENANCE from silver.erp_px_cat_g1v2;

--check for NULL's or Negetive Numbers
select DISTINCT prd_cost from silver.crm_prd_info where prd_cost < 0 OR prd_cost IS NULL;

select sls_sales from silver.crm_sales_details where sls_sales <= 0 or sls_sales IS NULL;
select sls_quantity from silver.crm_sales_details where sls_quantity <= 0 or sls_quantity IS NULL;
select sls_price from silver.crm_sales_details where sls_price <= 0 or sls_price IS NULL;

--Invalid Date Order
select * from silver.crm_prd_info where prd_start_dt > prd_end_dt and prd_end_dt IS NOT NULL;

select * from silver.crm_sales_details where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

--Invalid Dates
select sls_order_dt from silver.crm_sales_details where sls_order_dt <= 0 OR sls_order_dt < 19450101 OR LEN(sls_order_dt) != 8 OR sls_order_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR);
select sls_ship_dt from silver.crm_sales_details where sls_ship_dt <= 0 OR sls_ship_dt < 19450101 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR);
select sls_due_dt from silver.crm_sales_details where sls_due_dt <= 0 OR sls_due_dt < 19450101 OR LEN(sls_due_dt) != 8 OR sls_due_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR);

--Check Data Consistency According to Bussiness Rules
select sls_sales, sls_quantity, sls_price from silver.crm_sales_details where sls_sales != sls_price * sls_quantity;

--Check for Out of Range Dates
select BDATE from silver.erp_cust_az12 where BDATE < '1908-08-21' or BDATE > GETDATE();
