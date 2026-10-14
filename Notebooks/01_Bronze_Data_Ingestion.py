# Databricks notebook source
# Cell 1 - Secure Azure Blob Storage Connection

from azure.storage.blob import BlobServiceClient

storage_account = "retailsalesdlsb"
container_name = "retail-data"

# Retrieve SAS token from Databricks Secrets
sas_token = dbutils.secrets.get(
    scope="retail-sales",
    key="azure-sas-token"
)

# Remove invisible null characters
sas_token = sas_token.replace("\x00", "").strip().lstrip("?")

# Create secure Azure connection
blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account}.blob.core.windows.net",
    credential=sas_token
)

container_client = blob_service_client.get_container_client(
    container_name
)

# Verify connection
next(container_client.list_blobs(results_per_page=1), None)

print("Azure Blob Storage connected successfully!")

# COMMAND ----------

from io import BytesIO
from pyspark.sql.types import StringType

def load_csv_from_azure(filename):
    blob_client = container_client.get_blob_client(
        f"raw/{filename}"
    )

    csv_data = blob_client.download_blob().readall()

    pandas_df = pd.read_csv(
        BytesIO(csv_data),
        dtype=str
    )

    return spark.createDataFrame(pandas_df)


import pandas as pd

# Load datasets directly from Azure Blob Storage
customers_df = load_csv_from_azure("customers.csv")
products_df = load_csv_from_azure("products.csv")
sales_df = load_csv_from_azure("sales.csv")

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



# COMMAND ----------

# Cell 6 - Safe Bronze Sales Ingestion

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sales_table = "workspace.bronze.sales"

# Step 1: Prepare sales data from Azure Blob Storage
sales_source = (
    sales_df
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

# Step 2: Remove records with missing or invalid required values
sales_source = sales_source.filter(
    F.col("transaction_id").isNotNull() &
    (F.trim(F.col("transaction_id")) != "") &
    F.col("customer_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("quantity").isNotNull() &
    F.col("sale_date").isNotNull()
)

# Step 3: Remove duplicate transaction IDs from source
sales_source = sales_source.dropDuplicates(["transaction_id"])

# Step 4: Check whether Bronze sales table already exists
if spark.catalog.tableExists(sales_table):

    # Insert only transactions that do not already exist
    (
        DeltaTable.forName(spark, sales_table)
        .alias("target")
        .merge(
            sales_source.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("SUCCESS: New sales transactions merged into Bronze!")

else:

    # Create Bronze sales table for the first time
    (
        sales_source.write
        .format("delta")
        .mode("errorifexists")
        .saveAsTable(sales_table)
    )

    print("SUCCESS: Bronze sales table created!")

# Step 5: Display total Bronze sales records
print(
    "Total Bronze Sales:",
    spark.table(sales_table).count()
)

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))

# COMMAND ----------

from pyspark.sql.functions import to_date

sales_df = sales_df.withColumn(
    "sale_date",
    to_date("sale_date")
)

sales_df.printSchema()

# COMMAND ----------

print("Bronze Customers:", spark.table("workspace.bronze.customers").count())
print("Bronze Products:", spark.table("workspace.bronze.products").count())
print("Bronze Sales:", spark.table("workspace.bronze.sales").count())
