# Retail Sales ETL Lakehouse Pipeline using Databricks

## Project Overview

This project implements an end-to-end Retail Sales ETL pipeline using
Databricks and the Medallion Architecture. It ingests retail sales data,
processes it through Bronze, Silver, and Gold layers, supports
incremental loading, and updates business analytics when new files
arrive. A file-arrival trigger automates the workflow.

## Project Objective

-   Ingest raw retail sales data
-   Store raw data in the Bronze layer
-   Clean and standardize data in the Silver layer
-   Create business-level aggregations in the Gold layer
-   Support incremental data ingestion
-   Prevent duplicate transactions
-   Automatically trigger processing when new files arrive
-   Generate analytics for revenue, products, customers, and categories

## Architecture

``` text
Azure Blob Storage / Incoming CSV Files
                |
                v
        Databricks Volume
                |
                v
      Bronze Data Ingestion
                |
                v
        Bronze Delta Tables
                |
                v
       Silver Data Cleaning
                |
                v
        Silver Delta Tables
                |
                v
     Gold Business Analytics
                |
                v
         Gold Delta Tables
                |
                v
        Sales Analytics
                |
                v
     Business Visualizations
```

## Medallion Architecture

### Bronze Layer

Stores raw and incrementally ingested data.

Tables: - `workspace.bronze.customers` - `workspace.bronze.products` -
`workspace.bronze.sales`

### Silver Layer

Contains cleaned and standardized data.

Tables: - `workspace.silver.customers` - `workspace.silver.products` -
`workspace.silver.sales`

Transformations include data type conversion, date standardization, null
filtering, duplicate handling, data validation, and schema consistency.

### Gold Layer

Contains business-ready aggregated datasets.

Tables: - `workspace.gold.monthly_revenue` -
`workspace.gold.product_revenue` - `workspace.gold.category_revenue` -
`workspace.gold.customer_spending`

## Databricks Notebooks

### 01_Bronze_Data_Ingestion

Reads the initial raw datasets, loads customers/products/sales data, and
creates Bronze Delta tables.

### 01B_Incremental_Sales_Ingestion

Reads incoming sales CSV files, standardizes them, identifies new
transactions, prevents duplicates, and incrementally merges records into
Bronze.

``` text
Read Incoming CSV
      |
      v
Standardize Data
      |
      v
Identify New Transactions
      |
      v
MERGE into Bronze
      |
      v
Validate Bronze Table
```

### 02_Silver_Data_Cleaning

Reads Bronze data, cleans and standardizes records, handles data-quality
issues, and writes cleaned customers, products, and sales tables to
Silver.

### 03_Gold_Business_Analytics

Reads Silver tables, performs joins and aggregations, and creates
business-ready Gold tables.

### 04_Sales_Analytics

Reads Gold tables and provides the final business queries and
visualizations.

## Incremental Loading

Incremental loading uses Delta Lake `MERGE`, with `transaction_id` as
the unique transaction key.

``` sql
MERGE INTO workspace.bronze.sales AS target
USING new_sales_clean AS source
ON target.transaction_id = source.transaction_id
WHEN NOT MATCHED THEN
    INSERT *
```

This makes repeated processing idempotent for already-existing
transaction IDs.

Reusable validation:

``` sql
SELECT
    COUNT(*) AS total_bronze_sales,
    MAX(sale_date) AS latest_sale_date
FROM workspace.bronze.sales;
```

## Mixed Date Format Handling

Incoming files contained multiple date formats such as `9/29/2026` and
`2026-09-30`. The pipeline standardizes them before loading.

``` python
new_sales_clean = (
    new_sales_raw
    .withColumn(
        "sale_date",
        F.coalesce(
            F.expr("try_to_date(sale_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(sale_date, 'M/d/yyyy')"),
            F.expr("try_to_date(sale_date, 'MM/dd/yyyy')")
        )
    )
)
```

## File Arrival Trigger

The pipeline monitors:

``` text
/Volumes/workspace/default/retail_sales_incoming/
```

When a new CSV file arrives, Databricks starts the ETL workflow.

``` text
New CSV File
      |
      v
File Arrival Trigger
      |
      v
Incremental Sales Ingestion
      |
      v
Silver Cleaning
      |
      v
Gold Analytics
```

## Business Analytics

### Monthly Revenue

Uses `workspace.gold.monthly_revenue` to track revenue by month.

### Product Revenue

Uses `workspace.gold.product_revenue` to identify the highest
revenue-generating products.

### Category Revenue

Uses `workspace.gold.category_revenue` to analyze revenue across
categories such as Electronics, Furniture, Clothing, Home, Accessories,
and Stationery.

### Customer Spending

Uses `workspace.gold.customer_spending` to identify the highest-spending
customers.

## Final Analytics Notebook

`04_Sales_Analytics` contains: - Monthly Revenue Trend - Top 10 Products
by Revenue - Revenue by Category - Top 10 Customers by Spending

## Validation

The complete workflow was tested using incremental sales files.
Validation confirmed that: - The file-arrival trigger executed -
Incremental ingestion completed successfully - New transactions reached
Bronze - Silver tables refreshed - Gold aggregations updated - Final
analytics reflected the new data

A validation batch containing transactions `T523` through `T532`
successfully propagated through Bronze, Silver, Gold, and the analytics
layer.

## Technologies Used

-   Databricks
-   Apache Spark
-   PySpark
-   Spark SQL
-   Delta Lake
-   Unity Catalog
-   Databricks Workflows
-   File Arrival Triggers
-   Azure Blob Storage
-   CSV
-   SQL
-   Python

## Key Features

-   End-to-end ETL pipeline
-   Medallion Architecture
-   Bronze, Silver, and Gold layers
-   Incremental ingestion
-   Delta Lake `MERGE`
-   Duplicate prevention
-   File-arrival automation
-   Data validation
-   Mixed date-format handling
-   Business aggregations
-   Data visualizations
-   Automated downstream processing

## Challenges Solved

### Schema Mismatch

The Bronze sales table used a `DATE` column while incoming CSV dates
could arrive as strings. Incoming dates were standardized before
merging.

### Multiple Date Formats

Different incoming date formats were handled using `try_to_date()` with
multiple parsing patterns.

### Duplicate Records

Repeated files could introduce duplicate transactions. Delta Lake
`MERGE` using `transaction_id` prevents already-existing transaction IDs
from being inserted again.

### Temporary View Issues

Python DataFrames needed to be exposed to SQL cells using:

``` python
createOrReplaceTempView()
```

### Automated Execution

Manual pipeline execution was improved by configuring a Databricks File
Arrival Trigger.

## Project Outcome

The final solution demonstrates a cloud-based data engineering workflow
supporting initial ingestion, incremental ingestion, automated
orchestration, cleaning and standardization, Delta Lake storage,
business aggregation, and analytics/reporting.

The pipeline is reusable for future incoming retail sales data.

## Future Enhancements

-   Auto Loader for larger-scale incremental or streaming ingestion
-   Formal data-quality rules
-   Quarantine tables for invalid records
-   Slowly Changing Dimensions
-   Databricks SQL dashboards
-   Monitoring and alerting
-   Schema evolution
-   CI/CD integration
-   Additional KPI reporting

## Author

**Sandeep Babu**

Data Engineering Project
