# SQL Warehouse Project (SCD2)

This repository implements a simple Slowly Changing Dimension Type 2 (SCD2) data warehouse project. The goal is to demonstrate the staging, transformation, and loading processes involved in building and maintaining a data warehouse that tracks historical changes in dimension data.

> **Project root:** `SQL_Warehouse_Project_SCD2`

---

## 📁 Directory Structure

```
SQL_Warehouse_Project_SCD2/
├── datasets/                # original source data (CRM and ERP)
├── FileForLoading_scd2/     # sample batch folders used for loading
│   ├── batch1/              # first batch of source files
│   └── batch2/              # second batch of source files
├── docs/                    # documentation (if any)
├── scripts/                 # SQL scripts for DDL and data loads
│   ├── init_data_warehouse.sql
│   ├── report/              # reporting DDL / procedures
│   ├── srg0/                # source-ready stage 0 scripts
│   └── stage/               # stage-level schema and load scripts
└── tests/                   # data quality and stage check scripts
```

## 🔧 Purpose & Concepts

- **SCD Type 2**: Preserves full history of changes to dimension records by inserting new rows for changes and marking previous rows as obsolete.
- **Data flow**:
  1. Raw CSV files appear in `datasets/` or `FileForLoading_scd2/`.
  2. `scripts/srg0` handles the initial staging (`stg0`) of the source files.
  3. `scripts/stage` defines the SCD2-enabled schemas and load procedures.
  4. `tests` contain SQL queries to validate cleanliness and correctness.

## 🚀 Getting Started

1. **Set up your database** (SQL Server or compatible).
2. Run the `scripts/init_data_warehouse.sql` script to create the base objects and database.
3. Load the first batch by executing the procedures in `scripts/srg0/ddl_stg0.sql` and `scripts/srg0/proc_load_stg0.sql`.
4. Proceed to stage loads using `scripts/stage/ddl_stage_scd2.sql` and `scripts/stage/proc_load_stage_scd2.sql`.
5. Use the `tests` scripts to perform data quality checks.

## 📄 Reporting

- Use the scripts in `scripts/report/` for DDL and procedures to support reporting on the SCD2 data.

## 📝 Notes

- Adjust connection strings and environment-specific settings in the SQL scripts as needed.
- This project is intended as a learning resource; modify it to fit production needs.
