# Databricks notebook source

# STEP 1: Read Incoming Sales Files
# =================================

incoming_path = "/Volumes/workspace/default/retail_sales_incoming/"

new_sales_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(incoming_path)
)

new_sales_raw.createOrReplaceTempView("new_sales_raw")

print("Incoming records:", new_sales_raw.count())
display(new_sales_raw)

# COMMAND ----------

# STEP 2: Clean and Convert Incoming Sales Data
# ==============================================

from pyspark.sql import functions as F

new_sales_clean = (
    new_sales_raw
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn(
        "sale_date",
        F.coalesce(
            F.expr("try_to_date(sale_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(sale_date, 'M/d/yyyy')"),
            F.expr("try_to_date(sale_date, 'MM/dd/yyyy')")
        )
    )
)

# Remove records where important fields could not be parsed
new_sales_clean = new_sales_clean.filter(
    F.col("transaction_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("quantity").isNotNull() &
    F.col("sale_date").isNotNull()
)

# Make the cleaned DataFrame available to SQL cells
new_sales_clean.createOrReplaceTempView("new_sales_clean")

print("Clean incoming records:", new_sales_clean.count())
display(new_sales_clean)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- STEP 3: Identify New Transactions
# MAGIC -- ==================================
# MAGIC
# MAGIC SELECT
# MAGIC     n.transaction_id,
# MAGIC     n.customer_id,
# MAGIC     n.product_id,
# MAGIC     n.quantity,
# MAGIC     n.sale_date
# MAGIC FROM new_sales_clean n
# MAGIC LEFT ANTI JOIN workspace.bronze.sales b
# MAGIC     ON n.transaction_id = b.transaction_id
# MAGIC ORDER BY n.transaction_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- STEP 4: Incremental Load into Bronze
# MAGIC -- =====================================
# MAGIC
# MAGIC MERGE INTO workspace.bronze.sales AS target
# MAGIC USING new_sales_clean AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- STEP 5: Verify Bronze Sales Table
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_bronze_sales,
# MAGIC     MAX(sale_date) AS latest_sale_date
# MAGIC FROM workspace.bronze.sales;