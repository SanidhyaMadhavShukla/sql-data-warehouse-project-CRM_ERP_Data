/*
------------------------------------
Insert Data into Bronze Layer Tables (DML Script)
------------------------------------
Script Purpose:
	This script creates and executes a procedure that inserts data into tables in the 'bronze' schema from source, it truncates tables before loading data.

WARNING:
	Running this script will Truncate the tables leading to loss of all data stored in them.
*/

USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	--Load Data into Tables from Source
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_crm\cust_info.csv' 
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_crm\prd_info.csv' 
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_crm\sales_details.csv' 
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_erp\CUST_AZ12.csv' 
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM  'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\sanid\Desktop\365 Plan\Skills\Data Analytics\SQL\Data Warehouse Project (CRM-ERP Data)\Datasets\source_erp\PX_CAT_G1V2.csv' 
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
END;

EXEC bronze.load_bronze;
