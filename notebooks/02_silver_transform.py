# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver transform
# MAGIC Cleans the raw `bronze` tables into typed, deduplicated `silver` Delta tables.
# MAGIC
# MAGIC - **Types:** strings → timestamps / integers / doubles (using `try_cast`, so bad values become NULL instead of failing the job)
# MAGIC - **Standardisation:** trimmed text, lower-case cities, upper-case states, 5-digit zip codes, fixed column-name typos
# MAGIC - **Deduplication:** one row per business key, keeping the most recent ingest
# MAGIC - **Enrichment:** English product category names joined in
# MAGIC - **Lineage:** `_load_date` carried from bronze, `_processed_at` added
# MAGIC - **Incremental writes:** each row gets a `_row_hash`; tables are updated with Delta **MERGE**, so only
# MAGIC   new or changed rows are written and get a fresh `_processed_at`. Gold uses that timestamp as its watermark.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# COMMAND ----------

# MAGIC %md ## Helper functions

# COMMAND ----------

def clean_str(col_name):
    """Trim whitespace; turn empty strings into NULL."""
    c = F.trim(F.col(col_name))
    return F.when(c == "", None).otherwise(c)


def to_int(col_name):
    return F.expr(f"try_cast(trim({col_name}) AS INT)")


def to_double(col_name):
    return F.expr(f"try_cast(trim({col_name}) AS DOUBLE)")


def to_ts(col_name):
    return F.expr(f"try_cast(trim({col_name}) AS TIMESTAMP)")


def zip5(col_name):
    """Zip prefixes lose leading zeros in some files: pad back to 5 digits."""
    return F.lpad(clean_str(col_name), 5, "0")


def dedup(df, keys, order_cols=None):
    """Keep one row per business key: the latest by order_cols (default: _ingested_at)."""
    order_cols = order_cols or [F.col("_ingested_at").desc()]
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


results = []

def row_hash(cols):
    """Fingerprint of the business columns: changes whenever any value changes."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def save(df, table, rows_in, keys=None):
    """Write a silver table.

    First run (or tables without a key): full overwrite.
    Later runs: Delta MERGE on the business key:
      - changed rows (different _row_hash) are updated
      - new rows are inserted
      - rows no longer in the source snapshot are deleted
    Unchanged rows are not touched, so their _processed_at stays the same.
    """
    target = f"{SILVER}.{table}"
    business_cols = [c for c in df.columns if not c.startswith("_")]
    df = (df.withColumn("_row_hash", row_hash(business_cols))
            .withColumn("_processed_at", F.current_timestamp()))

    can_merge = (keys is not None and spark.catalog.tableExists(target)
                 and "_row_hash" in spark.table(target).columns)

    if can_merge:
        df.createOrReplaceTempView("silver_src")
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        out = spark.sql(f"""
            MERGE INTO {target} t
            USING silver_src s
            ON {on}
            WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN DELETE
        """).collect()
        m = out[0].asDict() if out else {}
        write = (f"MERGE: {m.get('num_inserted_rows', 0):,} ins / "
                 f"{m.get('num_updated_rows', 0):,} upd / {m.get('num_deleted_rows', 0):,} del")
    else:
        (df.write.mode("overwrite").format("delta")
           .option("overwriteSchema", "true")
           .saveAsTable(target))
        write = "full overwrite"

    rows_out = spark.table(target).count()
    results.append((table, rows_in, rows_out, rows_in - rows_out, write))
    print(f"{target}: {rows_in:,} in → {rows_out:,} rows  [{write}]")

# COMMAND ----------

# MAGIC %md ## Customers & sellers

# COMMAND ----------

raw = spark.table(f"{BRONZE}.customers")
customers = dedup(
    raw.select(
        clean_str("customer_id").alias("customer_id"),
        clean_str("customer_unique_id").alias("customer_unique_id"),
        zip5("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
        F.lower(clean_str("customer_city")).alias("customer_city"),
        F.upper(clean_str("customer_state")).alias("customer_state"),
        "_ingested_at", "_load_date",
    ).filter("customer_id IS NOT NULL"),
    ["customer_id"],
)
save(customers, "customers", raw.count(), ["customer_id"])

raw = spark.table(f"{BRONZE}.sellers")
sellers = dedup(
    raw.select(
        clean_str("seller_id").alias("seller_id"),
        zip5("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.lower(clean_str("seller_city")).alias("seller_city"),
        F.upper(clean_str("seller_state")).alias("seller_state"),
        "_ingested_at", "_load_date",
    ).filter("seller_id IS NOT NULL"),
    ["seller_id"],
)
save(sellers, "sellers", raw.count(), ["seller_id"])

# COMMAND ----------

# MAGIC %md ## Geolocation
# MAGIC The raw file has ~1M rows with many exact duplicates and a few coordinates outside Brazil.

# COMMAND ----------

raw = spark.table(f"{BRONZE}.geolocation")
geolocation = (
    raw.select(
        zip5("geolocation_zip_code_prefix").alias("zip_code_prefix"),
        to_double("geolocation_lat").alias("lat"),
        to_double("geolocation_lng").alias("lng"),
        F.lower(clean_str("geolocation_city")).alias("city"),
        F.upper(clean_str("geolocation_state")).alias("state"),
        "_load_date",
    )
    .filter("zip_code_prefix IS NOT NULL")
    # Brazil's bounding box: drops bad coordinates
    .filter(F.col("lat").between(-34.0, 5.5) & F.col("lng").between(-74.0, -34.5))
    .dropDuplicates(["zip_code_prefix", "lat", "lng", "city", "state"])
)
save(geolocation, "geolocation", raw.count())   # no business key → always overwrite

# COMMAND ----------

# MAGIC %md ## Orders

# COMMAND ----------

raw = spark.table(f"{BRONZE}.orders")
orders = dedup(
    raw.select(
        clean_str("order_id").alias("order_id"),
        clean_str("customer_id").alias("customer_id"),
        F.lower(clean_str("order_status")).alias("order_status"),
        to_ts("order_purchase_timestamp").alias("order_purchase_ts"),
        to_ts("order_approved_at").alias("order_approved_ts"),
        to_ts("order_delivered_carrier_date").alias("order_delivered_carrier_ts"),
        to_ts("order_delivered_customer_date").alias("order_delivered_customer_ts"),
        to_ts("order_estimated_delivery_date").alias("order_estimated_delivery_ts"),
        "_ingested_at", "_load_date",
    ).filter("order_id IS NOT NULL"),
    ["order_id"],
)
save(orders, "orders", raw.count(), ["order_id"])

# COMMAND ----------

# MAGIC %md ## Order items, payments, reviews

# COMMAND ----------

raw = spark.table(f"{BRONZE}.order_items")
order_items = dedup(
    raw.select(
        clean_str("order_id").alias("order_id"),
        to_int("order_item_id").alias("order_item_id"),
        clean_str("product_id").alias("product_id"),
        clean_str("seller_id").alias("seller_id"),
        to_ts("shipping_limit_date").alias("shipping_limit_ts"),
        to_double("price").alias("price"),
        to_double("freight_value").alias("freight_value"),
        "_ingested_at", "_load_date",
    ).filter("order_id IS NOT NULL AND order_item_id IS NOT NULL"),
    ["order_id", "order_item_id"],
)
save(order_items, "order_items", raw.count(), ["order_id", "order_item_id"])

raw = spark.table(f"{BRONZE}.order_payments")
order_payments = dedup(
    raw.select(
        clean_str("order_id").alias("order_id"),
        to_int("payment_sequential").alias("payment_sequential"),
        F.lower(clean_str("payment_type")).alias("payment_type"),
        to_int("payment_installments").alias("payment_installments"),
        to_double("payment_value").alias("payment_value"),
        "_ingested_at", "_load_date",
    ).filter("order_id IS NOT NULL AND payment_sequential IS NOT NULL"),
    ["order_id", "payment_sequential"],
)
save(order_payments, "order_payments", raw.count(), ["order_id", "payment_sequential"])

# The same review can appear more than once: keep the most recently answered version
raw = spark.table(f"{BRONZE}.order_reviews")
order_reviews = dedup(
    raw.select(
        clean_str("review_id").alias("review_id"),
        clean_str("order_id").alias("order_id"),
        to_int("review_score").alias("review_score"),
        clean_str("review_comment_title").alias("review_comment_title"),
        clean_str("review_comment_message").alias("review_comment_message"),
        to_ts("review_creation_date").alias("review_creation_ts"),
        to_ts("review_answer_timestamp").alias("review_answer_ts"),
        "_ingested_at", "_load_date",
    ).filter("review_id IS NOT NULL AND order_id IS NOT NULL"),
    ["review_id", "order_id"],
    [F.col("review_answer_ts").desc_nulls_last(), F.col("_ingested_at").desc()],
)
save(order_reviews, "order_reviews", raw.count(), ["review_id", "order_id"])

# COMMAND ----------

# MAGIC %md ## Products (+ English category names)
# MAGIC Fixes the source typos (`lenght` → `length`) and fills missing categories with `unknown`.

# COMMAND ----------

translation = (
    spark.table(f"{BRONZE}.category_translation")
    .select(
        clean_str("product_category_name").alias("product_category_name"),
        clean_str("product_category_name_english").alias("product_category_name_english"),
    )
    .filter("product_category_name IS NOT NULL")
    .dropDuplicates(["product_category_name"])
)
save(translation, "category_translation",
     spark.table(f"{BRONZE}.category_translation").count(), ["product_category_name"])

raw = spark.table(f"{BRONZE}.products")
products = dedup(
    raw.select(
        clean_str("product_id").alias("product_id"),
        clean_str("product_category_name").alias("product_category_name"),
        to_int("product_name_lenght").alias("product_name_length"),
        to_int("product_description_lenght").alias("product_description_length"),
        to_int("product_photos_qty").alias("product_photos_qty"),
        to_double("product_weight_g").alias("product_weight_g"),
        to_double("product_length_cm").alias("product_length_cm"),
        to_double("product_height_cm").alias("product_height_cm"),
        to_double("product_width_cm").alias("product_width_cm"),
        "_ingested_at", "_load_date",
    ).filter("product_id IS NOT NULL"),
    ["product_id"],
)
products = (
    products.join(translation, "product_category_name", "left")
    .withColumn("product_category_name",
                F.coalesce("product_category_name", F.lit("unknown")))
    .withColumn("product_category_name_english",
                F.coalesce("product_category_name_english",
                           F.col("product_category_name")))
)
save(products, "products", raw.count(), ["product_id"])

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

summary = spark.createDataFrame(results, "table STRING, rows_in LONG, rows_out LONG, rows_removed LONG, write STRING")
display(summary) if "display" in globals() else summary.show(truncate=False)
total_out = sum(r[2] for r in results)
print(f"TOTAL silver rows: {total_out:,}")
