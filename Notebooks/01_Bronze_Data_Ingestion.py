# Databricks notebook source
customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/default/retail_sales_raw/customers.csv")
)

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

customers_df.count()

# COMMAND ----------

products_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/default/retail_sales_raw/products.csv")
)

display(products_df)

# COMMAND ----------

sales_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/default/retail_sales_raw/sales.csv")
)

display(sales_df)

# COMMAND ----------

print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Sales:", sales_df.count())

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.customers")

# COMMAND ----------

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.products")

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.sales")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))

# COMMAND ----------

storage_account = "retailsalesdlsb"
container = "retail-data"

sas_token = dbutils.secrets.get(
    scope="retail-sales",
    key="azure-sas-token"
).replace("\x00", "").strip().lstrip("?")

customers_url = (
    f"https://{storage_account}.blob.core.windows.net/"
    f"{container}/raw/customers.csv?{sas_token}"
)

print("Azure customers.csv URL created successfully")

# COMMAND ----------

import requests
import pandas as pd
from io import StringIO

response = requests.get(customers_url)

print("HTTP Status:", response.status_code)

if response.status_code == 200:
    customers_pd = pd.read_csv(StringIO(response.text))
    customers_df = spark.createDataFrame(customers_pd)

    print("Successfully loaded customers.csv from Azure!")
    print("Rows:", customers_df.count())

    display(customers_df)
else:
    print("Failed to access Azure file")
    print(response.text[:500])

# COMMAND ----------

import requests
import pandas as pd
from io import StringIO

def load_csv_from_azure(filename):
    url = (
        f"https://{storage_account}.blob.core.windows.net/"
        f"{container}/raw/{filename}?{sas_token}"
    )

    response = requests.get(url)
    response.raise_for_status()

    pandas_df = pd.read_csv(StringIO(response.text))

    return spark.createDataFrame(pandas_df)


# Load all three datasets
customers_df = load_csv_from_azure("customers.csv")
products_df = load_csv_from_azure("products.csv")
sales_df = load_csv_from_azure("sales.csv")

print("Customers:", customers_df.count())
print("Products:", products_df.count())
print("Sales:", sales_df.count())

# COMMAND ----------

print("Azure sales dataframe:")
sales_df.printSchema()

print("Existing Bronze sales table:")
spark.table("workspace.bronze.sales").printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

sales_df = sales_df.withColumn(
    "sale_date",
    to_date("sale_date")
)

sales_df.printSchema()

# COMMAND ----------

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.sales")

print("Bronze sales table updated successfully!")

# COMMAND ----------

print("Bronze Customers:", spark.table("workspace.bronze.customers").count())
print("Bronze Products:", spark.table("workspace.bronze.products").count())
print("Bronze Sales:", spark.table("workspace.bronze.sales").count())

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/retail_sales_raw/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.bronze.sales
# MAGIC WHERE transaction_id BETWEEN 'T523' AND 'T532'
# MAGIC ORDER BY transaction_id;
