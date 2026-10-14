# Databricks notebook source
customers_bronze = spark.table("workspace.bronze.customers")
products_bronze = spark.table("workspace.bronze.products")
sales_bronze = spark.table("workspace.bronze.sales")

# COMMAND ----------

print("Customers:", customers_bronze.count())
print("Products:", products_bronze.count())
print("Sales:", sales_bronze.count())

# COMMAND ----------

customers_bronze.groupBy(
    "customer_id",
    "customer_name",
    "city",
    "state"
).count().filter("count > 1").show()

# COMMAND ----------

from pyspark.sql import functions as F

customers_silver = (
    spark.table("workspace.bronze.customers")
    .dropDuplicates(["customer_id"])
    .withColumn(
        "customer_name",
        F.when(
            F.col("customer_name").isNull() |
            (F.trim(F.col("customer_name")) == ""),
            "Unknown"
        ).otherwise(F.col("customer_name"))
    )
    .withColumn(
        "city",
        F.when(
            F.col("city").isNull() |
            (F.trim(F.col("city")) == ""),
            "Unknown"
        ).otherwise(F.col("city"))
    )
)

print("Cleaned Customers:", customers_silver.count())

# COMMAND ----------

from pyspark.sql import functions as F

invalid_customers = customers_silver.filter(
    F.col("customer_id").isNull() |
    (F.trim(F.col("customer_id")) == "") |
    F.col("customer_name").isNull() |
    (F.trim(F.col("customer_name")) == "") |
    F.col("city").isNull() |
    (F.trim(F.col("city")) == "") |
    F.col("state").isNull() |
    (F.trim(F.col("state")) == "")
)

duplicate_ids = (
    customers_silver
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Total Silver Customers:", customers_silver.count())
print("Invalid Customer Records:", invalid_customers.count())
print("Duplicate Customer IDs:", duplicate_ids.count())

# COMMAND ----------

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.customers")

print(
    "Silver Customers Saved:",
    spark.table("workspace.silver.customers").count()
)

# COMMAND ----------

from pyspark.sql import functions as F

products_bronze = spark.table("workspace.bronze.products")

duplicate_products = (
    products_bronze
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Total Bronze Products:", products_bronze.count())
print("Duplicate Product IDs:", duplicate_products.count())

display(duplicate_products)

# COMMAND ----------

from pyspark.sql import functions as F

products_silver = spark.table("workspace.bronze.products")

# Check duplicate product IDs
duplicate_ids = (
    products_silver
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

# Check missing product details
invalid_details = products_silver.filter(
    F.col("product_id").isNull() |
    (F.trim(F.col("product_id")) == "") |
    F.col("product_name").isNull() |
    (F.trim(F.col("product_name")) == "") |
    F.col("category").isNull() |
    (F.trim(F.col("category")) == "")
)

# Check missing or invalid prices
invalid_prices = products_silver.filter(
    F.col("price").isNull() |
    (F.trim(F.col("price").cast("string")) == "") |
    F.expr("try_cast(price AS DOUBLE)").isNull() |
    (F.expr("try_cast(price AS DOUBLE)") <= 0)
)

print("Total Products:", products_silver.count())
print("Duplicate Product IDs:", duplicate_ids.count())
print("Invalid Product Details:", invalid_details.count())
print("Invalid Prices:", invalid_prices.count())

# COMMAND ----------

print("Duplicate Product Records:")
display(
    products_silver.join(
        duplicate_ids.select("product_id"),
        on="product_id",
        how="inner"
    )
)

print("Invalid Price Records:")
display(invalid_prices)

# COMMAND ----------

from pyspark.sql import functions as F

products_cleaned = (
    spark.table("workspace.bronze.products")
    .dropDuplicates(["product_id"])
    .withColumn("price", F.col("price").cast("double"))
    
)

print("Original Products:", spark.table("workspace.bronze.products").count())
print("Cleaned Products:", products_cleaned.count())

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

products_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.products")

print(
    "Silver Products Saved:",
    spark.table("workspace.silver.products").count()
)

# COMMAND ----------

from pyspark.sql import functions as F

sales_bronze = spark.table("workspace.bronze.sales")

print("Total Bronze Sales:", sales_bronze.count())

print(
    "Duplicate Transaction IDs:",
    sales_bronze.count() -
    sales_bronze.select("transaction_id").distinct().count()
)

print(
    "Missing Required Values:",
    sales_bronze.filter(
        F.col("transaction_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("product_id").isNull() |
        F.col("quantity").isNull() |
        F.col("sale_date").isNull()
    ).count()
)

# COMMAND ----------

print("Silver Customers:", spark.table("workspace.silver.customers").count())
print("Silver Products:", spark.table("workspace.silver.products").count())
print("Silver Sales:", spark.table("workspace.silver.sales").count())

# COMMAND ----------

from pyspark.sql import functions as F

invalid_sales = sales_bronze.filter(
    F.col("transaction_id").isNull() |
    (F.trim(F.col("transaction_id")) == "") |
    F.col("customer_id").isNull() |
    (F.trim(F.col("customer_id")) == "") |
    F.col("product_id").isNull() |
    (F.trim(F.col("product_id")) == "") |
    F.col("quantity").isNull() |
    (F.col("quantity") <= 0) |
    F.col("sale_date").isNull()
)

print("Total Bronze Sales:", sales_bronze.count())
print("Invalid Sales Records:", invalid_sales.count())

display(invalid_sales)

# COMMAND ----------

from pyspark.sql import functions as F

missing_customers = sales_bronze.join(
    spark.table("workspace.silver.customers").select("customer_id"),
    on="customer_id",
    how="left_anti"
)

missing_products = sales_bronze.join(
    spark.table("workspace.silver.products").select("product_id"),
    on="product_id",
    how="left_anti"
)

print("Total Bronze Sales:", sales_bronze.count())
print("Sales with Missing Customers:", missing_customers.count())
print("Sales with Missing Products:", missing_products.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Read Bronze Sales
sales_bronze = spark.table("workspace.bronze.sales")

# Prepare cleaned Silver Sales
sales_silver = (
    sales_bronze
    .dropDuplicates(["transaction_id"])
    .withColumn("quantity", F.col("quantity").cast("double"))
    .withColumn("sale_date", F.col("sale_date").cast("date"))
)

# Overwrite Silver Sales with the updated schema
sales_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.sales")

# Verify saved records
print(
    "Silver Sales Saved:",
    spark.table("workspace.silver.sales").count()
)

print("SUCCESS: Silver Sales table created!")

# COMMAND ----------

# Read saved Silver tables
customers = spark.table("workspace.silver.customers")
products = spark.table("workspace.silver.products")
sales = spark.table("workspace.silver.sales")

# Verify record counts
print("Silver Customers:", customers.count())
print("Silver Products:", products.count())
print("Silver Sales:", sales.count())

# Verify important data types
print("\nSales Quantity Type:", sales.schema["quantity"].dataType)
print("Sales Date Type:", sales.schema["sale_date"].dataType)
print("Product Price Type:", products.schema["price"].dataType)

# COMMAND ----------

products_bronze.groupBy(
    "product_id",
    "product_name",
    "category",
    "price"
).count().filter("count > 1").show()

# COMMAND ----------

sales_bronze.groupBy(
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "sale_date"
).count().filter("count > 1").show()

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ============================================
# MAGIC -- VERIFY NEW RECORDS IN SILVER
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.silver.sales
# MAGIC WHERE transaction_id BETWEEN 'T523' AND 'T532'
# MAGIC ORDER BY transaction_id;

# COMMAND ----------

print("Silver Customers:", spark.table("workspace.silver.customers").count())
print("Silver Products:", spark.table("workspace.silver.products").count())
print("Silver Sales:", spark.table("workspace.silver.sales").count())
