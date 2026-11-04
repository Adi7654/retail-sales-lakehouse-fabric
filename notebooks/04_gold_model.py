# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold model (star schema)
# MAGIC Builds the reporting layer from `silver`:
# MAGIC
# MAGIC | Table | Type | How it is loaded |
# MAGIC |---|---|---|
# MAGIC | `fact_sales` | Fact, 1 row per order item | Delta **MERGE** upsert on `(order_id, order_item_id)` |
# MAGIC | `dim_customer` | Dimension, **SCD Type 2** | MERGE that expires the old version and inserts the new one |
# MAGIC | `dim_product`, `dim_seller` | Dimension, SCD Type 1 | MERGE upsert (overwrite changed attributes) |
# MAGIC | `dim_date` | Calendar dimension | Rebuilt each run (small) |
# MAGIC | `load_log` | Run history | Mode, duration, watermark, rows processed |
# MAGIC
# MAGIC **Modes** (widget at the top):
# MAGIC - `full`: rebuild everything from silver
# MAGIC - `incremental` (default): only process silver rows whose `_processed_at` is newer than the last
# MAGIC   successful run's **watermark**. Falls back to `full` automatically on the first run.

# COMMAND ----------

import time
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
LOG = f"{GOLD}.load_log"

dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"], "Load mode")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

# MAGIC %md ## Setup: mode, watermark, helpers

# COMMAND ----------

last_wm = None
if mode == "incremental":
    if spark.catalog.tableExists(LOG) and spark.catalog.tableExists(f"{GOLD}.fact_sales"):
        last_wm = spark.sql(f"SELECT max(watermark) FROM {LOG} WHERE status = 'success'").collect()[0][0]
    if last_wm is None:
        print("No previous successful load found → running a FULL load instead.")
        mode = "full"

run_id = str(uuid.uuid4())
run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
RUN_TS = f"TIMESTAMP'{run_ts:%Y-%m-%d %H:%M:%S}'"   # same timestamp used for every SCD2 change in this run

SOURCES = ["customers", "orders", "order_items", "products", "sellers"]
# New watermark = newest silver change visible right now (processed by this run)
new_wm = max(
    spark.sql(f"SELECT max(_processed_at) FROM {SILVER}.{t}").collect()[0][0] for t in SOURCES
)

print(f"Mode: {mode.upper()} | last watermark: {last_wm} | new watermark: {new_wm}")


def changed(table):
    """All silver rows (full) or only rows changed since the last watermark (incremental)."""
    df = spark.table(f"{SILVER}.{table}")
    if mode == "incremental":
        df = df.filter(F.col("_processed_at") > F.lit(last_wm))
    return df


def row_hash(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def overwrite(df, table):
    (df.write.mode("overwrite").format("delta")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{GOLD}.{table}"))


def run_merge(sql):
    out = spark.sql(sql).collect()
    m = out[0].asDict() if out else {}
    return {k: m.get(k, 0) for k in ["num_inserted_rows", "num_updated_rows"]}


stats = {}
start = time.time()

# COMMAND ----------

# MAGIC %md ## dim_date (calendar)

# COMMAND ----------

bounds = spark.sql(f"""
    SELECT to_date(min(order_purchase_ts)) AS d_min,
           to_date(greatest(max(order_purchase_ts), max(order_estimated_delivery_ts))) AS d_max
    FROM {SILVER}.orders
""").collect()[0]

dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{bounds.d_min}', DATE'{bounds.d_max}', INTERVAL 1 DAY)) AS date")
    .select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
)
overwrite(dim_date, "dim_date")

# COMMAND ----------

# MAGIC %md ## dim_seller & dim_product (SCD Type 1: keep latest values)

# COMMAND ----------

def upsert_type1(src, table, key, attrs):
    src = (src.select(key, *attrs)
              .withColumn("attr_hash", row_hash(attrs))
              .withColumn("_loaded_at", F.current_timestamp()))
    if mode == "full":
        overwrite(src, table)
        return {"num_inserted_rows": src.count(), "num_updated_rows": 0}
    src.createOrReplaceTempView("t1_src")
    return run_merge(f"""
        MERGE INTO {GOLD}.{table} t
        USING t1_src s
        ON t.{key} = s.{key}
        WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


stats["dim_seller"] = upsert_type1(
    changed("sellers"), "dim_seller", "seller_id",
    ["seller_zip_code_prefix", "seller_city", "seller_state"],
)
stats["dim_product"] = upsert_type1(
    changed("products"), "dim_product", "product_id",
    ["product_category_name", "product_category_name_english", "product_name_length",
     "product_description_length", "product_photos_qty", "product_weight_g",
     "product_length_cm", "product_height_cm", "product_width_cm"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer (SCD Type 2: keep history)
# MAGIC When a customer's address changes, the current row is **expired** (`is_current = false`,
# MAGIC `effective_to` set) and a **new version** is inserted with a new surrogate `customer_key`.
# MAGIC
# MAGIC Trick: the source is staged twice. Rows with `merge_key = customer_id` match and expire the old
# MAGIC version; changed rows with `merge_key = NULL` never match, so they are inserted as the new version.
# MAGIC Both happen in **one atomic MERGE**.

# COMMAND ----------

CUST_ATTRS = ["customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]
cust_src = (changed("customers")
            .select("customer_id", *CUST_ATTRS)
            .withColumn("attr_hash", row_hash(CUST_ATTRS)))

if mode == "full":
    dim_customer = (cust_src
        .withColumn("customer_key", F.xxhash64("customer_id", F.lit("1900-01-01")))
        .withColumn("effective_from", F.lit("1900-01-01 00:00:00").cast("timestamp"))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .select("customer_key", "customer_id", *CUST_ATTRS, "attr_hash",
                "effective_from", "effective_to", "is_current"))
    overwrite(dim_customer, "dim_customer")
    stats["dim_customer"] = {"num_inserted_rows": dim_customer.count(), "num_updated_rows": 0}
else:
    cust_src.createOrReplaceTempView("cust_src")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW cust_staged AS
        SELECT s.customer_id AS merge_key, s.* FROM cust_src s
        UNION ALL
        SELECT NULL AS merge_key, s.*
        FROM cust_src s
        JOIN {GOLD}.dim_customer t
          ON s.customer_id = t.customer_id AND t.is_current
        WHERE t.attr_hash <> s.attr_hash
    """)
    stats["dim_customer"] = run_merge(f"""
        MERGE INTO {GOLD}.dim_customer t
        USING cust_staged s
        ON t.customer_id = s.merge_key AND t.is_current
        WHEN MATCHED AND t.attr_hash <> s.attr_hash THEN
          UPDATE SET is_current = false, effective_to = {RUN_TS}
        WHEN NOT MATCHED THEN
          INSERT (customer_key, customer_id, customer_unique_id, customer_zip_code_prefix,
                  customer_city, customer_state, attr_hash, effective_from, effective_to, is_current)
          VALUES (xxhash64(s.customer_id, CAST({RUN_TS} AS STRING)), s.customer_id, s.customer_unique_id,
                  s.customer_zip_code_prefix, s.customer_city, s.customer_state, s.attr_hash,
                  {RUN_TS}, NULL, true)
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales (1 row per order item)
# MAGIC Incremental scope = orders that changed **or** whose items changed since the watermark.
# MAGIC New rows get the customer version that is **current at load time**. Updates to existing rows
# MAGIC (e.g. status → delivered) keep their original `customer_key`, so history stays correct.

# COMMAND ----------

items = spark.table(f"{SILVER}.order_items")
orders = spark.table(f"{SILVER}.orders")

if mode == "incremental":
    scope = (changed("orders").select("order_id")
             .union(changed("order_items").select("order_id"))
             .distinct())
    items = items.join(scope, "order_id", "left_semi")
    orders = orders.join(scope, "order_id", "left_semi")

current_customer = spark.table(f"{GOLD}.dim_customer").filter("is_current").select("customer_id", "customer_key")

MEASURE_COLS = ["customer_id", "product_id", "seller_id", "order_date_key", "order_purchase_ts",
                "order_status", "price", "freight_value", "item_total", "delivery_days",
                "is_delivered", "is_late"]

fact_src = (
    items.join(orders, "order_id")
    .join(current_customer, "customer_id", "left")
    .select(
        "order_id", "order_item_id", "customer_key", "customer_id", "product_id", "seller_id",
        F.date_format("order_purchase_ts", "yyyyMMdd").cast("int").alias("order_date_key"),
        "order_purchase_ts", "order_status", "price", "freight_value",
        F.round(F.col("price") + F.col("freight_value"), 2).alias("item_total"),
        F.datediff("order_delivered_customer_ts", "order_purchase_ts").alias("delivery_days"),
        (F.col("order_status") == "delivered").alias("is_delivered"),
        F.coalesce(F.col("order_delivered_customer_ts") > F.col("order_estimated_delivery_ts"),
                   F.lit(False)).alias("is_late"),
    )
    .withColumn("_row_hash", row_hash(MEASURE_COLS))
    .withColumn("_loaded_at", F.current_timestamp())
)

if mode == "full":
    overwrite(fact_src, "fact_sales")
    stats["fact_sales"] = {"num_inserted_rows": "all (full rebuild)", "num_updated_rows": 0}
else:
    fact_src.createOrReplaceTempView("fact_src")
    update_set = ", ".join(f"{c} = s.{c}" for c in MEASURE_COLS + ["_row_hash", "_loaded_at"])
    stats["fact_sales"] = run_merge(f"""
        MERGE INTO {GOLD}.fact_sales t
        USING fact_src s
        ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT *
    """)

duration = round(time.time() - start, 1)

# COMMAND ----------

# MAGIC %md ## Log the run & summary

# COMMAND ----------

fact_rows_in_scope = fact_src.count()
customers_in_scope = cust_src.count()

log_row = spark.createDataFrame(
    [(run_id, mode, run_ts, duration, new_wm, fact_rows_in_scope, customers_in_scope, "success")],
    "run_id STRING, mode STRING, run_ts TIMESTAMP, duration_sec DOUBLE, watermark TIMESTAMP, "
    "fact_rows_processed LONG, customer_rows_processed LONG, status STRING",
)
log_row.write.mode("append").format("delta").saveAsTable(LOG)

print(f"\n{mode.upper()} load finished in {duration}s "
      f"({fact_rows_in_scope:,} fact rows, {customers_in_scope:,} customer rows in scope)\n")
for table, s in stats.items():
    print(f"  {table:<13} inserted: {s['num_inserted_rows']}, updated: {s['num_updated_rows']}")

for t in ["fact_sales", "dim_customer", "dim_product", "dim_seller", "dim_date"]:
    print(f"  {GOLD}.{t}: {spark.table(f'{GOLD}.{t}').count():,} rows")

history = spark.sql(f"""
    SELECT mode, run_ts, duration_sec, fact_rows_processed, customer_rows_processed
    FROM {LOG} ORDER BY run_ts DESC LIMIT 10
""")
display(history) if "display" in globals() else history.show(truncate=False)
