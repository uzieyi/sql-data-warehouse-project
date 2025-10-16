/*
==========================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver
==========================================================================================
Script Purpose 
  This stored procedure performs the ETL (Extract, Transform and Load) Process to populate 
  the 'Silver' schema tables from the 'bronze'schema.

Action Performed
    -Truncate Silver tables 
    - Insert transformed and cleaned data from Bronze into Silver tables


Parameters;
  None
  This stored procedure doesn't accept any parameters or return any value 

Usage Example 
EXEC silver.load_silver
===========================================================================================
*/


-- Insert for all 6 silver tables 



CREATE OR ALTER PROCEDURE silver.load_silver AS 

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '========================================'
		PRINT 'Loading Silver Layer'
		PRINT '========================================'
	


		PRINT '----------------------------------------';
		PRINT 'Loading CRM TABLES';
		PRINT '----------------------------------------';
	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT 'INSERTING DATA INTO silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
	cst_id, 
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT 
		cst_id,
		cst_key,
		--Trimming, removing unwanted spaces
		TRIM(cst_firstname) AS cst_firstnmae,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 ELSE 'n/a' -- Normalise marital status values to readable format
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a' -- Normalise gender values to readable format
		END cst_gndr,
		cst_create_date

	FROM (
		SELECT  --Removed duplicate and filtering
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) t 
	WHERE flag_last = 1; -- Select the most recent Record

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'


	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT 'INSERTING DATA INTO silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5),'-','_' )AS cat_id, --Extract category ID
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,      -- Extract product key
		prd_nm,
		ISNULL (prd_cost, 0) AS prd_cost, --changing NULL to 0
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'na' 
		END AS prd_line, -- Map product line codes to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt, -- change DATETIME to DATE as there was no time in the Data
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1
			AS DATE) AS prd_end_dt -- Calculate end date as one day before the next start date
	FROM bronze.crm_prd_info 

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'


	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT 'INSERTING DATA INTO silver.crm_sales_details';
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
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL --check for invalid date value
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- conver string to date | data type casting
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL --check for invalid date value
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- conver string to date
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL --check for invalid date value
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- conver string to date
		END AS sls_due_dt, 


		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != (sls_quantity * sls_price )
				THEN ABS(sls_quantity) * ABS (sls_price)
				ELSE ABS(sls_sales)
			END AS sls_sales, -- Recalculating sales if original value is missing or incorrect ---applying business rules to fix errors in price


		sls_quantity,

		CASE WHEN sls_price is NULL OR sls_price <= 0
				THEN ABS(sls_sales) / NULLIF(ABS(sls_quantity), 0)
				ELSE ABS(sls_price)
			END AS sls_price -- Recalculating sales if original value is missing or incorrect ---applying business rules to fix errors in price
	FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'

	

		PRINT '----------------------------------------';
		PRINT 'Loading ERP TABLES';
		PRINT '----------------------------------------';

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT 'INSERTING DATA INTO silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	SELECT
	CASE WHEN cid LIKE 'NAS%'THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid, --Trim unwanted characters from the cid
	CASE WHEN  bdate > GETDATE() 	THEN NULL
		ELSE bdate
	END AS bdate, -- set future birthdate to null 
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a' 
	END AS gen--data normalisation for gender values and handle unknown cases
	FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'

	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT 'INSERTING DATA INTO silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 
	(cid, cntry)
	SELECT 
	REPLACE(cid, '-', ''), -- handled invalid values
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry -- Normalise and handle missing or blank country codes 
	FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'



	SET @start_time = GETDATE();
	PRINT 'TRUNCATING TABLE silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT 'INSERTING DATA INTO silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance )
	SELECT 
	id,
	cat,
	subcat,
	maintenance 
	FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>-----------'

		SET @batch_end_time = GETDATE();
		PRINT '>>>>>>>>>>>>>>>>>>>>>>'
		PRINT 'Loading Silver Layer is Completed'
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>'

	END TRY 

	BEGIN CATCH 
		PRINT '============================================'
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================================'
	END CATCH 
END
