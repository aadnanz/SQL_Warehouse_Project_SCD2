/*
===============================================================================
DDL Script: Create report Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'report' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'report' Tables
===============================================================================
*/


-- Drop table if exists
IF OBJECT_ID('report.dim_customers', 'U') IS NOT NULL
    DROP TABLE report.dim_customers;
GO

CREATE TABLE report.dim_customers (
    customer_key     INT IDENTITY(1,1) PRIMARY KEY, 
    customer_id      INT,
    customer_number  VARCHAR(50),
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    country          VARCHAR(50),
    marital_status   VARCHAR(50),
    gender           VARCHAR(50),
    birthdate        DATE,
    create_date      DATETIME,
	dwh_create_date DATETIME DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO


-- Drop table if exists
IF OBJECT_ID('report.dim_products', 'U') IS NOT NULL
    DROP TABLE report.dim_products;
GO

CREATE TABLE report.dim_products (
    product_key     INT IDENTITY(1,1) PRIMARY KEY,
    product_id      INT,
    product_number  VARCHAR(50),
    product_name    VARCHAR(50),
    category_id     VARCHAR(10),
    category        VARCHAR(50),
    subcategory     VARCHAR(50),
    maintenance     VARCHAR(50),
    cost            int,
    product_line    VARCHAR(50),
    start_date      DATE,
	dwh_create_date DATETIME DEFAULT GETDATE(),
    -- SCD2 Columns
    is_current          CHAR (1) NOT NULL DEFAULT 'A',
    modified_dt         DATE
);
GO


-- Drop table if exists
IF OBJECT_ID('report.fact_sales', 'U') IS NOT NULL
    DROP TABLE report.fact_sales;
GO

CREATE TABLE report.fact_sales (
    order_number   VARCHAR(50),
    product_key    INT,
    customer_key   INT,
    order_date     DATE,
    shipping_date  DATE,
    due_date       DATE,
    sales_amount   INT,
    quantity       INT,
    price          INT,
	dwh_create_date DATETIME DEFAULT GETDATE()
);
GO
