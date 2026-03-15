/*
===============================================================================
Stored Procedure: Load report Layer (stage -> report)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'report' schema tables from the 'stage' schema.
	Actions Performed:
		- Truncates stage tables.
		- Inserts transformed and cleansed data from stage into report tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC report.load_report_scd2;
===============================================================================
*/

CREATE OR ALTER PROCEDURE report.load_report_scd2 AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading report Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

/*==================================
Loading report.dim_customers
==================================*/
        SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into: report.dim_customers';


/* STEP 1: Prepare latest source data */
SELECT 
    ci.cst_id, ci.cst_key, ci.cst_firstname, ci.cst_lastname, la.cntry, ci.cst_marital_status,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen,'n/a')
    END AS gender, ca.bdate, ci.cst_create_date
INTO #source_data
FROM stage.crm_cust_info ci
LEFT JOIN stage.erp_cust_az12 ca
       ON ci.cst_key = ca.cid
LEFT JOIN stage.erp_loc_a101 la
       ON ci.cst_key = la.cid
       WHERE ci.is_current = 'A';


/* STEP 2: Identify changed records */
SELECT 
    s.*
INTO #changed_records
FROM #source_data s
JOIN report.dim_customers t
     ON s.cst_id = t.customer_id
     AND t.is_current = 'A'
WHERE
       ISNULL(t.first_name,'')        <> ISNULL(s.cst_firstname,'')
    OR ISNULL(t.last_name,'')         <> ISNULL(s.cst_lastname,'')
    OR ISNULL(t.country,'')           <> ISNULL(s.cntry,'')
    OR ISNULL(t.marital_status,'')    <> ISNULL(s.cst_marital_status,'')
    OR ISNULL(t.gender,'')            <> ISNULL(s.gender,'')
    OR ISNULL(t.birthdate,'1900-01-01') <> ISNULL(s.bdate,'1900-01-01');


/* STEP 3: Expire old versions */
UPDATE tgt
SET 
    tgt.is_current  = 'I',
    tgt.modified_dt = GETDATE()
FROM report.dim_customers tgt
JOIN #changed_records cr
     ON tgt.customer_id = cr.cst_id
WHERE tgt.is_current = 'A';

/* STEP 4: Insert new version for changed customers */
INSERT INTO report.dim_customers
(customer_id, customer_number, first_name, last_name, country, marital_status, gender, birthdate, create_date, dwh_create_date, is_current)
SELECT
    cr.cst_id, cr.cst_key, cr.cst_firstname, cr.cst_lastname, cr.cntry, cr.cst_marital_status, cr.gender, cr.bdate,
    cr.cst_create_date, GETDATE(), 'A'
FROM #changed_records cr;

/* STEP 5: Insert completely new customers */
INSERT INTO report.dim_customers
    (customer_id, customer_number, first_name, last_name, country, marital_status, gender,
    birthdate, create_date,dwh_create_date, is_current)
SELECT
    s.cst_id, s.cst_key, s.cst_firstname, s.cst_lastname, s.cntry, s.cst_marital_status, s.gender,
    s.bdate, s.cst_create_date, GETDATE(), 'A'
FROM #source_data s
WHERE NOT EXISTS
            (SELECT 1
            FROM report.dim_customers t
            WHERE t.customer_id = s.cst_id);

/* CLEANUP */
DROP TABLE #source_data;
DROP TABLE #changed_records;


SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/*==================================
 Loading report.dim_products
==================================*/
SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into: report.dim_products';

/* STEP 1: Prepare Source Products */
SELECT pn.prd_id, pn.prd_key, pn.prd_nm, pn.cat_id, pc.cat, pc.subcat, pc.maintenance,
       pn.prd_cost, pn.prd_line, pn.prd_start_dt
INTO #source_products
FROM stage.crm_prod_info pn
LEFT JOIN stage.erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

/* STEP 2: Detect Changed Products */
SELECT s.*
INTO #changed_products
FROM #source_products s
JOIN report.dim_products t
     ON s.prd_id = t.product_id
    AND t.is_current = 'A'
WHERE
       ISNULL(t.product_number,'') <> ISNULL(s.prd_key,'')
    OR ISNULL(t.product_name,'') <> ISNULL(s.prd_nm,'')
    OR ISNULL(t.category_id,'') <> ISNULL(s.cat_id,'')
    OR ISNULL(t.category,'') <> ISNULL(s.cat,'')
    OR ISNULL(t.subcategory,'') <> ISNULL(s.subcat,'')
    OR ISNULL(t.maintenance,'') <> ISNULL(s.maintenance,'')
    OR ISNULL(t.cost,0) <> ISNULL(s.prd_cost,0)
    OR ISNULL(t.product_line,'') <> ISNULL(s.prd_line,'')
    OR ISNULL(t.start_date,'1900-01-01') <> ISNULL(s.prd_start_dt,'1900-01-01');

/* STEP 3: Expire Old Active Records */
UPDATE tgt
SET
    tgt.is_current = 'I',
    tgt.modified_dt = GETDATE()
FROM report.dim_products tgt
JOIN #changed_products c
     ON tgt.product_id = c.prd_id
WHERE tgt.is_current = 'A';

/* STEP 4: Insert New Version for Changed Products */
INSERT INTO report.dim_products
    (product_id, product_number, product_name, category_id, category, subcategory,
    maintenance, cost, product_line, start_date, is_current)
SELECT c.prd_id, c.prd_key, c.prd_nm, c.cat_id, c.cat, c.subcat,
    c.maintenance, c.prd_cost, c.prd_line, c.prd_start_dt, 'A'
FROM #changed_products c;



/* STEP 5: Insert Completely New Products */
INSERT INTO report.dim_products
    (product_id, product_number, product_name, category_id, category, subcategory,
    maintenance, cost, product_line, start_date, is_current)
SELECT s.prd_id, s.prd_key, s.prd_nm, s.cat_id, s.cat, s.subcat, s.maintenance,
    s.prd_cost, s.prd_line, s.prd_start_dt, 'A'
FROM #source_products s
WHERE NOT EXISTS
    (SELECT 1
    FROM report.dim_products t
    WHERE t.product_id = s.prd_id);

/* STEP 6: Cleanup */
DROP TABLE #source_products;
DROP TABLE #changed_products;


SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


/*==================================
Loading crm_sales_details
==================================*/
        SET @start_time = GETDATE();
		PRINT '>> Inserting Data Into: report.fact_sales';

-- Step 1: Load staging with dedupe
IF OBJECT_ID('tempdb..#sales_staging') IS NOT NULL DROP TABLE #sales_staging;

SELECT DISTINCT
    sd.sls_ord_num, pr.product_key, cu.customer_key, sd.sls_order_dt, sd.sls_ship_dt,
    sd.sls_due_dt, sd.sls_sales, sd.sls_quantity, sd.sls_price
INTO #sales_staging
FROM stage.crm_sales_details sd
LEFT JOIN report.dim_products pr
    ON sd.sls_prd_key = pr.product_number
    AND pr.is_current = 'A'
LEFT JOIN report.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id
    AND cu.is_current = 'A';

-- Step 2: Insert only new records
INSERT INTO report.fact_sales(
    order_number, product_key, customer_key, order_date, 
    shipping_date, due_date, sales_amount, quantity, price
)
SELECT
    ss.sls_ord_num, ss.product_key, ss.customer_key, ss.sls_order_dt, ss.sls_ship_dt, ss.sls_due_dt,
    ss.sls_sales, ss.sls_quantity, ss.sls_price
FROM #sales_staging ss
WHERE NOT EXISTS (
    SELECT 1 
    FROM report.fact_sales fs 
    WHERE fs.order_number = ss.sls_ord_num
      AND COALESCE(fs.product_key, 0) = COALESCE(ss.product_key, 0)
      AND COALESCE(fs.customer_key, 0) = COALESCE(ss.customer_key, 0)
      AND fs.order_date = ss.sls_order_dt
);

-- Cleanup
DROP TABLE #sales_staging;

SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

    END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING REPORT LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
