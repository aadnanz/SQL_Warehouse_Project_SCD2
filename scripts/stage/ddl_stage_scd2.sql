/*
===============================================================================
DDL Script: Create stage Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'stage' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'stage' Tables
    This DDL statement can work with Slowly Changing Dimension Type2
===============================================================================
*/

IF OBJECT_ID('stage.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE stage.crm_cust_info;
GO

CREATE TABLE stage.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date		DATETIME2 DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);


IF OBJECT_ID('stage.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE stage.crm_prod_info;
GO

CREATE TABLE stage.crm_prod_info (
    prd_id				INT,
	cat_id				VARCHAR(50),
    prd_key				VARCHAR(50),
    prd_nm				VARCHAR(50),
    prd_cost			INT,
    prd_line			VARCHAR(50),
    prd_start_dt		DATE,
    prd_end_dt			DATE,
	dwh_create_date		DATETIME2 DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO

IF OBJECT_ID('stage.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE stage.crm_sales_details;
GO

CREATE TABLE stage.crm_sales_details (
    sls_ord_num			VARCHAR(50),
    sls_prd_key			VARCHAR(50),
    sls_cust_id			INT,
    sls_order_dt		DATE,
    sls_ship_dt			DATE,
    sls_due_dt			DATE,
    sls_sales			INT,
    sls_quantity		INT,
    sls_price			INT,
	dwh_create_date		DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('stage.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE stage.erp_loc_a101;
GO

CREATE TABLE stage.erp_loc_a101 (
    cid					VARCHAR(50),
    cntry				VARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO

IF OBJECT_ID('stage.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE stage.erp_cust_az12;
GO

CREATE TABLE stage.erp_cust_az12 (
    cid					VARCHAR(50),
    bdate				DATE,
    gen					VARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO

IF OBJECT_ID('stage.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE stage.erp_px_cat_g1v2;
GO

CREATE TABLE stage.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	dwh_create_date		DATETIME2 DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO
