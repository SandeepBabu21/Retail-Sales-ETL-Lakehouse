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

customers_clean = customers_bronze.dropDuplicates()

products_clean = products_bronze.dropDuplicates()

sales_clean = sales_bronze.dropDuplicates()

# COMMAND ----------

print("Customers before:", customers_bronze.count())
print("Customers after :", customers_clean.count())

print("Products before :", products_bronze.count())
print("Products after  :", products_clean.count())

print("Sales before    :", sales_bronze.count())
print("Sales after     :", sales_clean.count())

# COMMAND ----------

from pyspark.sql.functions import col, sum

customers_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in customers_clean.columns
]).show()

# COMMAND ----------

products_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in products_clean.columns
]).show()

# COMMAND ----------

sales_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in sales_clean.columns
]).show()

# COMMAND ----------

customers_clean = customers_clean.fillna({
    "customer_name": "Unknown",
    "city": "Unknown"
})

# COMMAND ----------

products_clean = products_clean.dropna(
    subset=["product_id", "product_name", "category", "price"]
)

# COMMAND ----------

sales_clean = sales_clean.dropna(
    subset=["transaction_id", "customer_id", "product_id", "quantity", "sale_date"]
)

# COMMAND ----------

print("Customers:", customers_clean.count())
print("Products:", products_clean.count())
print("Sales:", sales_clean.count())

# COMMAND ----------

customers_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in customers_clean.columns
]).show()

products_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in products_clean.columns
]).show()

sales_clean.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in sales_clean.columns
]).show()

# COMMAND ----------

customers_clean.printSchema()
products_clean.printSchema()
sales_clean.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date

products_clean = products_clean.withColumn(
    "price",
    col("price").cast("double")
)

sales_clean = (
    sales_clean
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
)

# COMMAND ----------

products_clean.printSchema()
sales_clean.printSchema()

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# COMMAND ----------

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers")

products_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.products")

sales_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.sales")

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