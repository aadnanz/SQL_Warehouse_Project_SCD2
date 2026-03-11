CREATE OR ALTER PROCEDURE stage.load_stage_scd2 AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading stage Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

/* ============================================
   Loading stage.crm_cust_info
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.crm_cust_info';

        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname)  AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date,
            CASE 
                WHEN ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) = 1 THEN 'A'
                ELSE 'I'
            END AS is_current
        INTO #crm_cust_info_temp
        FROM stg0.crm_cust_info
        WHERE cst_id IS NOT NULL;
        -- Deduplicate: keep only the latest active record per cst_id for MERGE
        SELECT 
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date, is_current
        INTO #crm_cust_info_dedup
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM #crm_cust_info_temp
        ) t
        WHERE rn = 1;
        -- STEP 1: Inactivate old active records that have changed
        MERGE stage.crm_cust_info AS tgt
        USING #crm_cust_info_dedup AS src
        ON tgt.cst_id = src.cst_id
        AND tgt.is_current = 'A'
        WHEN MATCHED AND (
                ISNULL(tgt.cst_key,'') <> ISNULL(src.cst_key,'') OR
                ISNULL(tgt.cst_firstname,'') <> ISNULL(src.cst_firstname,'') OR
                ISNULL(tgt.cst_lastname,'') <> ISNULL(src.cst_lastname,'') OR
                ISNULL(tgt.cst_marital_status,'') <> ISNULL(src.cst_marital_status,'') OR
                ISNULL(tgt.cst_gndr,'') <> ISNULL(src.cst_gndr,'') OR
                ISNULL(tgt.cst_create_date,'') <> ISNULL(src.cst_create_date,'')
            )
        THEN UPDATE SET
            tgt.is_current  = 'I',
            tgt.modified_dt = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (cst_id, cst_key, cst_firstname, cst_lastname, 
                    cst_marital_status, cst_gndr, cst_create_date, dwh_create_date, is_current)
            VALUES (src.cst_id, src.cst_key, src.cst_firstname, src.cst_lastname,
                    src.cst_marital_status, src.cst_gndr, src.cst_create_date, GETDATE(), 'A');

        -- STEP 2: Insert new active version for changed records
        INSERT INTO stage.crm_cust_info 
            (cst_id, cst_key, cst_firstname, cst_lastname, 
             cst_marital_status, cst_gndr, cst_create_date, is_current)
        SELECT 
            src.cst_id, src.cst_key, src.cst_firstname, src.cst_lastname,
            src.cst_marital_status, src.cst_gndr, src.cst_create_date, src.is_current
        FROM #crm_cust_info_dedup AS src
        INNER JOIN stage.crm_cust_info AS tgt
            ON tgt.cst_id      = src.cst_id
            AND tgt.is_current = 'I'
            AND tgt.modified_dt >= CAST(GETDATE() AS DATE)
        WHERE src.is_current = 'A';

        DROP TABLE #crm_cust_info_temp;
        DROP TABLE #crm_cust_info_dedup;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/* ============================================
   Loading stage.crm_prod_info
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.crm_prod_info';

        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key))         AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                AS DATE
            ) AS prd_end_dt,
            CASE WHEN CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) IS NULL THEN 'A'
            ELSE 'I' END AS is_current
        INTO #crm_prod_info_temp
        FROM stg0.crm_prod_info;
        -- Deduplicate: keep only the latest record per prd_id for MERGE
        SELECT
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        INTO #crm_prod_info_dedup
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS rn
            FROM #crm_prod_info_temp
            WHERE is_current = 'A'
        ) t
        WHERE rn = 1;
        -- STEP 1: Inactivate changed + insert new products
        MERGE stage.crm_prod_info AS tgt
        USING #crm_prod_info_dedup AS src
        ON tgt.prd_id = src.prd_id
        AND tgt.is_current = 'A'
        WHEN MATCHED AND (
                ISNULL(tgt.cat_id,'') <> ISNULL(src.cat_id,'') OR
                ISNULL(tgt.prd_key,'') <> ISNULL(src.prd_key,'') OR
                ISNULL(tgt.prd_nm,'') <> ISNULL(src.prd_nm,'') OR
                ISNULL(tgt.prd_cost,0) <> ISNULL(src.prd_cost,0) OR
                ISNULL(tgt.prd_line,'') <> ISNULL(src.prd_line,'') OR
                ISNULL(tgt.prd_start_dt,'') <> ISNULL(src.prd_start_dt,'') OR
                ISNULL(tgt.prd_end_dt,'') <> ISNULL(src.prd_end_dt,'')
            )
        THEN UPDATE SET
            tgt.is_current  = 'I',
            tgt.modified_dt = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line,
                    prd_start_dt, prd_end_dt, dwh_create_date, is_current)
            VALUES (src.prd_id, src.cat_id, src.prd_key, src.prd_nm, src.prd_cost,
                    src.prd_line, src.prd_start_dt, src.prd_end_dt, GETDATE(), 'A');

        -- STEP 2: Insert new active version for changed records
        INSERT INTO stage.crm_prod_info
            (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line,
             prd_start_dt, prd_end_dt, dwh_create_date, is_current)
        SELECT 
            src.prd_id, src.cat_id, src.prd_key, src.prd_nm, src.prd_cost,
            src.prd_line, src.prd_start_dt, src.prd_end_dt, GETDATE(), 'A'
        FROM #crm_prod_info_dedup AS src
        INNER JOIN stage.crm_prod_info AS tgt
            ON tgt.prd_id      = src.prd_id
            AND tgt.is_current = 'I'
            AND tgt.modified_dt >= CAST(GETDATE() AS DATE);

        DROP TABLE #crm_prod_info_temp;
        DROP TABLE #crm_prod_info_dedup;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/* ============================================
   Loading stage.crm_sales_details
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.crm_sales_details';

        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 
                     OR sls_sales <> sls_quantity * ABS(sls_price)
                     THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        INTO #crm_sales_details_temp
        FROM stg0.crm_sales_details;

        -- STEP 1: Removing duplicate from #crm_sales_details_temp
        WITH cte AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        ORDER BY sls_ord_num) rn
        FROM #crm_sales_details_temp)
        DELETE FROM cte
        WHERE rn > 1;

        -- STEP 2: Insert non duplicate records only
        INSERT INTO stage.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt,
        sls_due_dt, sls_sales, sls_quantity, sls_price, dwh_create_date)

        SELECT src.sls_ord_num, src.sls_prd_key, src.sls_cust_id, src.sls_order_dt, src.sls_ship_dt, src.sls_due_dt,
        src.sls_sales, src.sls_quantity, src.sls_price, GETDATE()
        FROM #crm_sales_details_temp src

        WHERE NOT EXISTS (
        SELECT 1
        FROM stage.crm_sales_details tgt
        WHERE tgt.sls_ord_num = src.sls_ord_num
        AND tgt.sls_prd_key = src.sls_prd_key
        AND tgt.sls_cust_id = src.sls_cust_id
        AND tgt.sls_order_dt = src.sls_order_dt
        AND tgt.sls_ship_dt = src.sls_ship_dt
        AND tgt.sls_due_dt = src.sls_due_dt
        AND tgt.sls_sales = src.sls_sales
        AND tgt.sls_quantity = src.sls_quantity
        AND tgt.sls_price = src.sls_price);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/* ============================================
   Loading stage.erp_cust_az12
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.erp_cust_az12';

        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        INTO #erp_cust_az12_temp
        FROM stg0.erp_cust_az12;

        -- Deduplicate: keep only the latest record per cid for MERGE
        SELECT cid, bdate, gen
        INTO #erp_cust_az12_dedup
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cid ORDER BY bdate DESC) AS rn
            FROM #erp_cust_az12_temp
        ) t
        WHERE rn = 1;

        -- STEP 1: Inactivate changed + insert new records
        MERGE stage.erp_cust_az12 AS tgt
        USING #erp_cust_az12_dedup AS src
        ON tgt.cid = src.cid
        AND tgt.is_current = 'A'
        WHEN MATCHED AND (
                ISNULL(tgt.bdate,'') <> ISNULL(src.bdate,'') OR
                ISNULL(tgt.gen,'') <> ISNULL(src.gen,'')
            )
        THEN UPDATE SET
            tgt.is_current  = 'I',
            tgt.modified_dt = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (cid, bdate, gen, dwh_create_date, is_current)
            VALUES (src.cid, src.bdate, src.gen, GETDATE(), 'A');

        -- STEP 2: Insert new active version for changed records
        INSERT INTO stage.erp_cust_az12 (cid, bdate, gen, dwh_create_date, is_current)
        SELECT src.cid, src.bdate, src.gen, GETDATE(), 'A'
        FROM #erp_cust_az12_dedup AS src
        INNER JOIN stage.erp_cust_az12 AS tgt
            ON tgt.cid         = src.cid
            AND tgt.is_current = 'I'
            AND tgt.modified_dt >= CAST(GETDATE() AS DATE);

        DROP TABLE #erp_cust_az12_temp;
        DROP TABLE #erp_cust_az12_dedup;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/* ============================================
   Loading stage.erp_loc_a101
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.erp_loc_a101';

        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        INTO #erp_loc_a101_temp
        FROM stg0.erp_loc_a101;

        -- Deduplicate: keep only one record per cid for MERGE
        SELECT cid, cntry
        INTO #erp_loc_a101_dedup
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cid ORDER BY cntry) AS rn
            FROM #erp_loc_a101_temp
        ) t
        WHERE rn = 1;

        -- STEP 1: Inactivate changed + insert new records
        MERGE stage.erp_loc_a101 AS tgt
        USING #erp_loc_a101_dedup AS src
        ON tgt.cid = src.cid
        AND tgt.is_current = 'A'
        WHEN MATCHED AND (
                ISNULL(tgt.cntry, '') <> ISNULL(src.cntry, '')
            )
        THEN UPDATE SET
            tgt.is_current  = 'I',
            tgt.modified_dt = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (cid, cntry, dwh_create_date, is_current)
            VALUES (src.cid, src.cntry, GETDATE(), 'A');

        -- STEP 2: Insert new active version for changed records
        INSERT INTO stage.erp_loc_a101 (cid, cntry, dwh_create_date, is_current)
        SELECT src.cid, src.cntry, GETDATE(), 'A'
        FROM #erp_loc_a101_dedup AS src
        INNER JOIN stage.erp_loc_a101 AS tgt
            ON tgt.cid         = src.cid
            AND tgt.is_current = 'I'
            AND tgt.modified_dt >= CAST(GETDATE() AS DATE);

        DROP TABLE #erp_loc_a101_temp;
        DROP TABLE #erp_loc_a101_dedup;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

/* ============================================
   Loading stage.erp_px_cat_g1v2
   ============================================ */
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: stage.erp_px_cat_g1v2';

        SELECT id, cat, subcat, maintenance
        INTO #erp_px_cat_g1v2_temp
        FROM stg0.erp_px_cat_g1v2;

        -- Deduplicate: keep only one record per id for MERGE
        SELECT id, cat, subcat, maintenance
        INTO #erp_px_cat_g1v2_dedup
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY cat) AS rn
            FROM #erp_px_cat_g1v2_temp
        ) t
        WHERE rn = 1;

        -- STEP 1: Inactivate changed + insert new records
        MERGE stage.erp_px_cat_g1v2 AS tgt
        USING #erp_px_cat_g1v2_dedup AS src
        ON tgt.id = src.id
        AND tgt.is_current = 'A'
        WHEN MATCHED AND (
                ISNULL(tgt.cat,'') <> ISNULL(src.cat,'') OR
                ISNULL(tgt.subcat,'') <> ISNULL(src.subcat,'') OR
                ISNULL(tgt.maintenance,'') <> ISNULL(src.maintenance,'')
            )
        THEN UPDATE SET
            tgt.is_current  = 'I',
            tgt.modified_dt = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (id, cat, subcat, maintenance, dwh_create_date, is_current)
            VALUES (src.id, src.cat, src.subcat, src.maintenance, GETDATE(), 'A');

        -- STEP 2: Insert new active version for changed records
        INSERT INTO stage.erp_px_cat_g1v2 (id, cat, subcat, maintenance, dwh_create_date, is_current)
        SELECT src.id, src.cat, src.subcat, src.maintenance, GETDATE(), 'A'
        FROM #erp_px_cat_g1v2_dedup AS src
        INNER JOIN stage.erp_px_cat_g1v2 AS tgt
            ON tgt.id          = src.id
            AND tgt.is_current = 'I'
            AND tgt.modified_dt >= CAST(GETDATE() AS DATE);

        DROP TABLE #erp_px_cat_g1v2_temp;
        DROP TABLE #erp_px_cat_g1v2_dedup;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading stage Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING stage LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
