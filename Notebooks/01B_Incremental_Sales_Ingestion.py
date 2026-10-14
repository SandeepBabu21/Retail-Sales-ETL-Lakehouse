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

from pyspark.sql import functions as F

# Step 1: Convert incoming sales data types
new_sales_clean = (
    new_sales_raw
    .withColumn("quantity", F.col("quantity").cast("double"))
    .withColumn(
        "sale_date",
        F.coalesce(
            F.expr("try_to_date(sale_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(sale_date, 'M/d/yyyy')"),
            F.expr("try_to_date(sale_date, 'MM/dd/yyyy')")
        )
    )
)

# Step 2: Remove invalid records
new_sales_clean = new_sales_clean.filter(
    F.col("transaction_id").isNotNull() &
    (F.trim(F.col("transaction_id")) != "") &
    F.col("customer_id").isNotNull() &
    (F.trim(F.col("customer_id")) != "") &
    F.col("product_id").isNotNull() &
    (F.trim(F.col("product_id")) != "") &
    F.col("quantity").isNotNull() &
    (F.col("quantity") > 0) &
    F.col("sale_date").isNotNull()
)

# Step 3: Remove duplicate transaction IDs
new_sales_clean = new_sales_clean.dropDuplicates(
    ["transaction_id"]
)

# Step 4: Create temporary view for SQL MERGE
new_sales_clean.createOrReplaceTempView("new_sales_clean")

# Step 5: Display results
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
# MAGIC MERGE INTO workspace.bronze.sales AS target
# MAGIC USING new_sales_clean AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     sale_date
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.transaction_id,
# MAGIC     source.customer_id,
# MAGIC     source.product_id,
# MAGIC     source.quantity,
# MAGIC     source.sale_date
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_bronze_sales,
# MAGIC     COUNT(DISTINCT transaction_id) AS unique_transactions,
# MAGIC     MAX(sale_date) AS latest_sale_date
# MAGIC FROM workspace.bronze.sales;
