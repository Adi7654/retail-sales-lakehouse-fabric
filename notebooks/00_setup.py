# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog objects the pipeline needs:
# MAGIC - Schemas: `bronze`, `silver`, `gold` (one per medallion layer)
# MAGIC - Volume: `bronze.landing`, the landing zone for raw files
# MAGIC - A date-stamped folder for today's load: `/Volumes/<catalog>/bronze/landing/olist/<YYYY-MM-DD>`
# MAGIC
# MAGIC Safe to re-run: every statement uses `IF NOT EXISTS`.

# COMMAND ----------

from datetime import date

CATALOG = "workspace"   # default catalog in Databricks Free Edition

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema ready: {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
print(f"Volume ready: {CATALOG}.bronze.landing")

# COMMAND ----------

load_date = date.today().isoformat()
landing_path = f"/Volumes/{CATALOG}/bronze/landing/olist/{load_date}"
dbutils.fs.mkdirs(landing_path)
print(f"Upload the raw CSVs to: {landing_path}")
