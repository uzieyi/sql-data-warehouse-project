-- Create Database 'DataWarehouse'
/*
==================
Create Database and Schema
===================
Script Purpose 
  To create a new database and schema for the datawarehouse project 
  It also set up 3 new schemes - 'bronze', 'silve' and 'gold'.

WARNING 
If you have a database with the same name, check and drop the existing database before running this script or use a different name 
*/

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

