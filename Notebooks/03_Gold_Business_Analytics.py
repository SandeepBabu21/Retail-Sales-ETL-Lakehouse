# Databricks notebook source
customers = spark.table("workspace.silver.customers")
products = spark.table("workspace.silver.products")
sales = spark.table("workspace.silver.sales")

# COMMAND ----------

print("Customers:", customers.count())
print("Products:", products.count())
print("Sales:", sales.count())

# COMMAND ----------

# Check whether every sale matches a product
sales_products_check = sales.join(
    products,
    on="product_id",
    how="left"
)

print("Total Silver Sales:", sales.count())
print("Sales after Product Join:", sales_products_check.count())

print(
    "Sales with Missing Prices:",
    sales_products_check.filter("price IS NULL").count()
)

# COMMAND ----------

from pyspark.sql import functions as F

sales_revenue_check = sales_products_check.withColumn(
    "revenue",
    F.col("quantity") * F.col("price")
)

print("Total Sales:", sales_revenue_check.count())

print(
    "Sales with Calculated Revenue:",
    sales_revenue_check.filter(F.col("revenue").isNotNull()).count()
)

print(
    "Sales with Unknown Revenue:",
    sales_revenue_check.filter(F.col("revenue").isNull()).count()
)

print(
    "Known Revenue:",
    sales_revenue_check.agg(
        F.round(F.sum("revenue"), 2).alias("total")
    ).first()["total"]
)

# COMMAND ----------

from pyspark.sql import functions as F

# Join Sales with Products, keeping all transactions
sales_products = sales.join(
    products,
    on="product_id",
    how="left"
)

# Calculate revenue, preserving NULL for missing prices
sales_products = sales_products.withColumn(
    "revenue",
    F.col("quantity") * F.col("price")
)

print("Sales after Product Join:", sales_products.count())

print(
    "Sales with Unknown Revenue:",
    sales_products.filter(F.col("revenue").isNull()).count()
)

display(sales_products)

# COMMAND ----------

print("Sales before join:", sales.count())
print("After product join:", sales_products.count())

# COMMAND ----------

# Join Sales and Products with Customers
# Keep all sales transactions

sales_complete = sales_products.join(
    customers,
    on="customer_id",
    how="left"
)

print("Total Sales:", sales.count())
print("Sales after Customer Join:", sales_complete.count())

print(
    "Sales with Missing Customers:",
    sales_complete.filter("customer_name IS NULL").count()
)

display(sales_complete)

# COMMAND ----------

from pyspark.sql import functions as F

# Validate revenue already calculated in Cell 5
print("Total Sales:", sales_complete.count())

print(
    "Sales with Known Revenue:",
    sales_complete.filter(F.col("revenue").isNotNull()).count()
)

print(
    "Sales with Unknown Revenue:",
    sales_complete.filter(F.col("revenue").isNull()).count()
)

print(
    "Total Known Revenue:",
    sales_complete.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue")
    ).first()["total_revenue"]
)

# COMMAND ----------

print("After product join:", sales_products.count())
print("After customer join:", sales_complete.count())

# COMMAND ----------

from pyspark.sql.functions import sum, round

total_revenue = sales_complete.agg(
    round(sum("revenue"), 2).alias("total_revenue")
)

display(total_revenue)

# COMMAND ----------

from pyspark.sql.functions import col, sum, round

product_revenue = (
    sales_complete
    .groupBy("product_id", "product_name")
    .agg(
        round(sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

display(product_revenue)

# COMMAND ----------

from pyspark.sql.functions import date_format, sum, round, col

monthly_revenue = (
    sales_complete
    .withColumn("month", date_format("sale_date", "yyyy-MM"))
    .groupBy("month")
    .agg(
        round(sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy("month")
)

display(monthly_revenue)

# COMMAND ----------

category_revenue = (
    sales_complete
    .groupBy("category")
    .agg(
        round(sum("revenue"), 2).alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

display(category_revenue)

# COMMAND ----------

customer_spending = (
    sales_complete
    .groupBy(
        "customer_id",
        "customer_name"
    )
    .agg(
        round(sum("revenue"), 2).alias("total_spent")
    )
    .orderBy(col("total_spent").desc())
)

display(customer_spending)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# COMMAND ----------

product_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.product_revenue")

monthly_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.monthly_revenue")

category_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.category_revenue")

customer_spending.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.gold.customer_spending")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.gold"))

# COMMAND ----------

print("Product Revenue:", spark.table("workspace.gold.product_revenue").count())
print("Monthly Revenue:", spark.table("workspace.gold.monthly_revenue").count())
print("Category Revenue:", spark.table("workspace.gold.category_revenue").count())
print("Customer Spending:", spark.table("workspace.gold.customer_spending").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ============================================
# MAGIC -- VERIFY GOLD MONTHLY REVENUE
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.monthly_revenue
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify all Gold analytics
# MAGIC
# MAGIC SELECT * FROM workspace.gold.category_revenue
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.product_revenue
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.customer_spending
# MAGIC ORDER BY total_spent DESC;
