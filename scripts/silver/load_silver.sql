/*
------------------------------------
Insert Data into silver Layer Tables (DML Script)
------------------------------------
Script Purpose:
	This script creates a procedure that inserts data into tables in the 'silver' schema from source, it truncates tables before loading data.

WARNING:
	Running this script will Truncate the tables leading to loss of all data stored in them.
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT('==============================================================');
		PRINT('Loading silver Layer');
		PRINT('==============================================================');

		--Load Data into Tables from Source
		PRINT('--------------------------------------------------------------');
		PRINT('Loading CRM Tables');
		PRINT('--------------------------------------------------------------');
		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT('-> Inserting Data Into: silver.crm_cust_info');
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select 
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		from 
		(
			select 
				cst_id,
				cst_key,
				TRIM(cst_firstname) as cst_firstname,
				TRIM(cst_lastname) as cst_lastname,
				CASE UPPER(TRIM(cst_marital_status))
					WHEN 'M' THEN 'Married'
					WHEN 'S' THEN 'Single'
					ELSE 'Unknown'
				END as cst_marital_status,
				CASE UPPER(TRIM(cst_gndr))
					WHEN 'M' THEN 'Male'
					WHEN 'F' THEN 'Female'
					ELSE 'Unknown'
				END as cst_gndr,
				cst_create_date,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as lastest_flag
			from bronze.crm_cust_info
			where cst_id IS NOT NULL
		) as silver_crm_cust_info
		where lastest_flag = 1;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';
		PRINT('->');

		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.crm_prd_info');
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT('-> Inserting Data Into: silver.crm_prd_info');
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			prd_cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		from
		(
			select 
				prd_id,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') as prd_cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
				prd_nm,
				COALESCE(prd_cost, 0) as prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other'
					WHEN 'T' THEN 'Touring'
					ELSE 'Unknown'
				END AS prd_line,
				prd_start_dt,
				DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt
			from bronze.crm_prd_info
		) as silver_crm_prd_info;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';
		PRINT('->');

		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT('-> Inserting Data Into: silver.crm_sales_details');
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		FROM (
			SELECT
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN 
						sls_order_dt < 19450101 OR 
						LEN(sls_order_dt) != 8 OR 
						sls_order_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR) 
					THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					WHEN 
						sls_ship_dt < 19450101 OR 
						LEN(sls_ship_dt) != 8 OR 
						sls_ship_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR) 
					THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					WHEN 
						sls_due_dt < 19450101 OR 
						LEN(sls_due_dt) != 8 OR 
						sls_due_dt > CAST(FORMAT(GETDATE(),'yyyyMMdd') AS NVARCHAR) 
					THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
				CASE
					WHEN
						sls_sales <= 0 or sls_sales IS NULL or sls_sales != ABS(sls_quantity) * ABS(sls_price)
					THEN
						ABS(sls_quantity) * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales,
				ABS(sls_quantity) AS sls_quantity,
				CASE
					WHEN
						sls_price <= 0 or sls_price IS NULL
					THEN
						ABS(sls_sales) / NULLIF(ABS(sls_quantity),0)
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details
		) AS silver_crm_sales_details;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';
		PRINT('->');

		PRINT('--------------------------------------------------------------');
		PRINT('Loading ERP Tables');
		PRINT('--------------------------------------------------------------');
		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.erp_cust_az12');
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT('-> Inserting Data Into: silver.erp_cust_az12');
		INSERT INTO silver.erp_cust_az12 (
			CID,
			BDATE,
			GEN
		)
		SELECT
			CID,
			BDATE,
			GEN
		FROM (
			SELECT 
				CASE 
					WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
					ELSE CID
				END AS CID,
				CASE
					WHEN BDATE > GETDATE() THEN NULL
					WHEN BDATE < '1909-08-21' THEN NULL --OLDEST PERSON ALIVE
					ELSE BDATE
				END AS BDATE,
				CASE
					WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
					ELSE 'Unkown'
				END AS GEN
			FROM bronze.erp_cust_az12
		) AS silver_erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';
		PRINT('->');

		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.erp_loc_a101');
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT('-> Inserting Data Into: silver.erp_loc_a101');
		INSERT INTO silver.erp_loc_a101 (
			CID,
			CNTRY
		)
		SELECT
			CID,
			CNTRY
		FROM (
			SELECT 
				REPLACE(CID,'-','') AS CID,
				CASE 
					WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'Germany'
					WHEN UPPER(TRIM(CNTRY)) IN ('USA','US','UNITED STATES') THEN 'United States of America'
					WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = '' THEN 'Unkown'
					ELSE TRIM(CNTRY)
				END AS CNTRY
			FROM bronze.erp_loc_a101
		) AS silver_erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';
		PRINT('->');

		SET @start_time = GETDATE();
		PRINT('-> Truncating Table: silver.erp_px_cat_g1v2');
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT('-> Inserting Data Into: silver.erp_px_cat_g1v2');
		INSERT INTO silver.erp_px_cat_g1v2 (
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM (
			select
				ID,
				CAT,
				SUBCAT,
				CASE UPPER(TRIM(MAINTENANCE))
					WHEN 'YES' THEN 1
					WHEN 'NO' THEN 0
					ELSE NULL
				END AS MAINTENANCE
			from bronze.erp_px_cat_g1v2
		) AS silver_erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT('-> Loading Time ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)) + 'seconds';

		SET @batch_end_time = GETDATE();
		PRINT('->Batch Loading Time ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)) + 'seconds';

		PRINT('==============================================================');
	END TRY
	BEGIN CATCH
		PRINT('=======================================================');
		PRINT('ERROR OCCURED DURING LOADING silver LAYER');
		PRINT('=======================================================');
		PRINT('ERROR MESSAGE: '+ERROR_MESSAGE());
		PRINT('ERROR NUMBER: '+ CAST (ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR STATE: '+ CAST (ERROR_STATE() AS NVARCHAR));
		PRINT('=======================================================');
	END CATCH
END;
