/*
===============================================================================
Stored Procedure: Load stg0 Layer (Source -> stg0)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'stg0' schema from external CSV files. 
    It performs the following actions:
    - Truncates the stg0 tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to stg0 tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC stg0.load_stg0;
===============================================================================
*/
CREATE OR ALTER PROCEDURE stg0.load_stg0 AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading stg0 Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.crm_cust_info';
		TRUNCATE TABLE stg0.crm_cust_info;
		PRINT '>> Inserting Data Into: stg0.crm_cust_info';
		BULK INSERT stg0.crm_cust_info
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.crm_prod_info';
		TRUNCATE TABLE stg0.crm_prod_info;

		PRINT '>> Inserting Data Into: stg0.crm_prd_info';
		BULK INSERT stg0.crm_prod_info
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.crm_sales_details';
		TRUNCATE TABLE stg0.crm_sales_details;
		PRINT '>> Inserting Data Into: stg0.crm_sales_details';
		BULK INSERT stg0.crm_sales_details
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.erp_loc_a101';
		TRUNCATE TABLE stg0.erp_loc_a101;
		PRINT '>> Inserting Data Into: stg0.erp_loc_a101';
		BULK INSERT stg0.erp_loc_a101
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.erp_cust_az12';
		TRUNCATE TABLE stg0.erp_cust_az12;
		PRINT '>> Inserting Data Into: stg0.erp_cust_az12';
		BULK INSERT stg0.erp_cust_az12
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: stg0.erp_px_cat_g1v2';
		TRUNCATE TABLE stg0.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: stg0.erp_px_cat_g1v2';
		BULK INSERT stg0.erp_px_cat_g1v2
		FROM 'C:\Users\Lenovo\Desktop\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading stg0 Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING stg0 LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
