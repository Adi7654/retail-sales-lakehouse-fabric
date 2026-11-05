# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Stream orders with Auto Loader
# MAGIC Incrementally ingests the order-event JSON files from the landing Volume into
# MAGIC `bronze.order_events`, then publishes **"today's sales"** views in `gold`.
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) tracks which files it has already read in a **checkpoint**,
# MAGIC   so every run picks up **only new files**, exactly once
# MAGIC - **Explicit schema**: types are enforced on read (no guessing)
# MAGIC - **Deduplication**: re-sent events (same `event_id`) are dropped with `dropDuplicatesWithinWatermark`
# MAGIC - **Trigger `availableNow`**: process everything new, then stop. This is the standard way to run a
# MAGIC   stream inside a scheduled job (serverless compute does not support always-on streams)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
EVENTS_PATH = f"/Volumes/{CATALOG}/bronze/landing/order_events"
TARGET = f"{CATALOG}.bronze.order_events"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/order_events"

EVENT_SCHEMA = """
    event_id STRING, order_id STRING, order_item_id INT, customer_id STRING,
    product_id STRING, seller_id STRING, price DOUBLE, freight_value DOUBLE,
    payment_type STRING, order_status STRING, event_ts TIMESTAMP
"""

rows_before = spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0

# COMMAND ----------

# MAGIC %md ## Auto Loader stream

# COMMAND ----------

events = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/_schema")
    .schema(EVENT_SCHEMA)
    .load(EVENTS_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    # Drop re-sent events. The watermark bounds how long event_ids are remembered.
    .withWatermark("event_ts", "1 hour")
    .dropDuplicatesWithinWatermark(["event_id"])
)

query = (
    events.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
)
query.awaitTermination()

rows_read = sum((p["numInputRows"] or 0) for p in query.recentProgress)   # some progress entries report None
rows_after = spark.table(TARGET).count()
new_rows = rows_after - rows_before

print(f"Events read this run:      {rows_read:,}")
print(f"New rows written:          {new_rows:,}  ({rows_read - new_rows:,} duplicates dropped)")
print(f"Total rows in {TARGET}: {rows_after:,}")

# COMMAND ----------

# MAGIC %md ## "Today's sales" views (gold)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.v_todays_sales AS
SELECT
    current_date()                                              AS sales_date,
    count(DISTINCT order_id)                                    AS orders,
    count(*)                                                    AS items,
    round(sum(price + freight_value), 2)                        AS revenue,
    round(sum(price + freight_value) / count(DISTINCT order_id), 2) AS avg_order_value,
    max(event_ts)                                               AS last_event_at
FROM {TARGET}
WHERE to_date(event_ts) = current_date()
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.v_todays_sales_by_hour AS
SELECT
    hour(event_ts)                        AS hour_utc,
    count(DISTINCT order_id)              AS orders,
    round(sum(price + freight_value), 2)  AS revenue
FROM {TARGET}
WHERE to_date(event_ts) = current_date()
GROUP BY hour(event_ts)
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.v_todays_top_categories AS
SELECT
    coalesce(p.product_category_name_english, 'unknown') AS category,
    count(DISTINCT e.order_id)                          AS orders,
    round(sum(e.price + e.freight_value), 2)            AS revenue
FROM {TARGET} e
LEFT JOIN {GOLD}.dim_product p ON e.product_id = p.product_id
WHERE to_date(e.event_ts) = current_date()
GROUP BY 1
""")

for v in ["v_todays_sales", "v_todays_top_categories"]:
    df = spark.table(f"{GOLD}.{v}")
    if v == "v_todays_top_categories":
        df = df.orderBy(F.desc("revenue")).limit(5)
    print(f"\n{GOLD}.{v}")
    display(df) if "display" in globals() else df.show(truncate=False)
