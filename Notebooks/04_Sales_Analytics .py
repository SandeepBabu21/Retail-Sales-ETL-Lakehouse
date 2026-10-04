# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.monthly_revenue
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     product_name,
# MAGIC     total_revenue
# MAGIC FROM workspace.gold.product_revenue
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     category,
# MAGIC     total_revenue
# MAGIC FROM workspace.gold.category_revenue
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     customer_name,
# MAGIC     total_spent
# MAGIC FROM workspace.gold.customer_spending
# MAGIC ORDER BY total_spent DESC
# MAGIC LIMIT 10;

# COMMAND ----------

