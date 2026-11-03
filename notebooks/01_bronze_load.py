# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze load
# MAGIC Loads the raw Olist CSVs from the landing Volume into `bronze` Delta tables.
# MAGIC
# MAGIC - Picks the **latest** date folder in the landing zone automatically
# MAGIC - Keeps every column as a **string**: bronze is an unchanged copy of the source, so it can always be replayed
# MAGIC - Adds audit columns: `_ingested_at`, `_source_file`, `_load_date`
# MAGIC - Overwrites each table, so re-running never creates duplicates

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/landing/olist"

# COMMAND ----------

# Pick the latest load folder (folders are named YYYY-MM-DD, so they sort by date)
folders = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(LANDING) if f.isDir())
load_date = folders[-1]
base = f"{LANDING}/{load_date}"
print(f"Loading from: {base}")

# COMMAND ----------

files = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

total = 0
for table, fname in files.items():
    df = (spark.read.option("header", True)
                    .option("multiLine", True)     # review comments contain line breaks
                    .option("escape", '"')
                    .csv(f"{base}/{fname}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))   # Unity Catalog-safe lineage
            .withColumn("_load_date", F.lit(load_date)))
    target = f"{CATALOG}.bronze.{table}"
    (df.write.mode("overwrite").format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(target))
    n = spark.table(target).count()
    total += n
    print(f"{target}: {n:,} rows")

print(f"\nTOTAL: {total:,} rows")
