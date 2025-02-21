/*
	Save Frquently used SQL cosde in stored procedures in database
*/
EXEC bronze.load_bronze
/* FROM here down execute first the execute the load above sql script*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE  @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT 'Loading Bronze Layer';

		PRINT 'Loading CRM Tables'
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'Location:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		SELECT COUNT(*) FROM bronze.crm_cust_info
		

		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_prd_info

		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.crm_sales_details
	
		PRINT 'Loading ERP Tables'
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_cust_az12

		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_loc_a101

		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\kusha\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2
		
		SET @batch_end_time= GETDATE();
		PRINT'Total Load Time:'+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +'seconds';
	/* Add try and catch It ensures error handling, data integrity and issue logging for easier debugging.*/
	END TRY
	BEGIN CATCH
		PRINT '==================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message'+ ERROR_MESSAGE();
	END CATCH
END
