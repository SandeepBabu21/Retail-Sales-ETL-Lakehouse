# Databricks notebook source
customers = spark.table("workspace.silver.customers")
products = spark.table("workspace.silver.products")
sales = spark.table("workspace.silver.sales")

# COMMAND ----------

print("Customers:", customers.count())
print("Products:", products.count())
print("Sales:", sales.count())

# COMMAND ----------

sales_products = sales.join(
    products,
    on="product_id",
    how="inner"
)

display(sales_products)

# COMMAND ----------

print("Sales before join:", sales.count())
print("After product join:", sales_products.count())

# COMMAND ----------

sales_complete = sales_products.join(
    customers,
    on="customer_id",
    how="inner"
)

display(sales_complete)

# COMMAND ----------

from pyspark.sql.functions import col, round

sales_complete = sales_complete.withColumn(
    "revenue",
    round(col("quantity") * col("price"), 2)
)

display(sales_complete)

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