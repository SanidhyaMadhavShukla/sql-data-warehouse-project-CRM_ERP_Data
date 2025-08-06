/*
------------------------------------
Create Database and Schemas
------------------------------------
Script Purpose:
	This script creates a databse named 'DataWarehouse' after checking it already exists. If database exits, it is dropped before creating database.
	Also, scripts creates three schemas according to medeallion architecture named 'bronze','silver', and 'gold'.

WARNING:
	Running this script will delete the database if it already exists leading to loss of all data stored in it.
*/

USE master;
GO

-- DROP and RECREATE the 'DataWarehouse' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--CREATE DATABASE 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--CREATE SCHEMAS
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
