# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Simulate a new data batch
# MAGIC Olist is a one-time historical snapshot. To test incremental loading and SCD Type 2 like a real
# MAGIC daily feed, this notebook creates a **new landing folder** containing the latest snapshot plus changes:
# MAGIC
# MAGIC - **500 customers move** to a new city (tests SCD Type 2 history in `dim_customer`)
# MAGIC - **1,000 new orders** with their items (half of them placed by the customers who moved)
# MAGIC - **300 shipped orders become delivered** (tests updates to existing fact rows)
# MAGIC
# MAGIC Then run the pipeline: bronze → silver (MERGE) → DQ checks → gold (incremental).

# COMMAND ----------

from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/landing/olist"

N_MOVES = 500
N_NEW_ORDERS = 1000
N_STATUS_UPDATES = 300

# COMMAND ----------

folders = sorted(f.name.rstrip("/") for f in dbutils.fs.ls(LANDING) if f.isDir())
src = f"{LANDING}/{folders[-1]}"
now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
batch = now.strftime("%Y-%m-%dT%H%M%S")      # sorts after plain YYYY-MM-DD folders
dst = f"{LANDING}/{batch}"
dbutils.fs.mkdirs(dst)

now_s = now.strftime("%Y-%m-%d %H:%M:%S")
eta_s = (now + timedelta(days=10)).strftime("%Y-%m-%d 00:00:00")
print(f"Source snapshot: {src}\nNew batch:       {dst}")


def read_src(fname):
    return (spark.read.option("header", True).option("multiLine", True)
                 .option("escape", '"').csv(f"{src}/{fname}"))


def write_csv(df, fname):
    # Written as a folder named like the file; the bronze loader reads it the same way
    (df.coalesce(1).write.mode("overwrite")
       .option("header", True).option("escape", '"')
       .csv(f"{dst}/{fname}"))

# COMMAND ----------

# MAGIC %md ## Unchanged files: copy as-is

# COMMAND ----------

UNCHANGED = ["olist_geolocation_dataset.csv", "olist_order_payments_dataset.csv",
             "olist_order_reviews_dataset.csv", "olist_products_dataset.csv",
             "olist_sellers_dataset.csv", "product_category_name_translation.csv"]
for f in UNCHANGED:
    dbutils.fs.cp(f"{src}/{f}", f"{dst}/{f}", recurse=True)
print(f"Copied {len(UNCHANGED)} unchanged files")

# COMMAND ----------

# MAGIC %md ## Customers: 500 move to Curitiba (PR)

# COMMAND ----------

customers = read_src("olist_customers_dataset.csv")
mover_ids = [r.customer_id for r in
             customers.filter("customer_state <> 'PR'").orderBy(F.rand(42))
                      .limit(N_MOVES).select("customer_id").collect()]
is_mover = F.col("customer_id").isin(mover_ids)

customers_out = customers.select(
    "customer_id", "customer_unique_id",
    F.when(is_mover, F.lit("80010")).otherwise(F.col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
    F.when(is_mover, F.lit("curitiba")).otherwise(F.col("customer_city")).alias("customer_city"),
    F.when(is_mover, F.lit("PR")).otherwise(F.col("customer_state")).alias("customer_state"),
)
write_csv(customers_out, "olist_customers_dataset.csv")

# COMMAND ----------

# MAGIC %md ## Orders: 1,000 new + 300 status updates

# COMMAND ----------

orders = read_src("olist_orders_dataset.csv")

# Templates for new orders: half from customers who moved, half random delivered orders
mover_orders = [r.order_id for r in orders.filter(F.col("customer_id").isin(mover_ids))
                .limit(N_NEW_ORDERS // 2).select("order_id").collect()]
other_orders = [r.order_id for r in
                orders.filter("order_status = 'delivered'")
                      .filter(~F.col("order_id").isin(mover_orders))
                      .orderBy(F.rand(7)).limit(N_NEW_ORDERS - len(mover_orders))
                      .select("order_id").collect()]
template_ids = mover_orders + other_orders
new_id = F.md5(F.concat(F.col("order_id"), F.lit(batch)))

new_orders = orders.filter(F.col("order_id").isin(template_ids)).select(
    new_id.alias("order_id"),
    "customer_id",
    F.lit("processing").alias("order_status"),
    F.lit(now_s).alias("order_purchase_timestamp"),
    F.lit(now_s).alias("order_approved_at"),
    F.lit(None).cast("string").alias("order_delivered_carrier_date"),
    F.lit(None).cast("string").alias("order_delivered_customer_date"),
    F.lit(eta_s).alias("order_estimated_delivery_date"),
)

delivered_ids = [r.order_id for r in
                 orders.filter("order_status = 'shipped'").orderBy(F.rand(3))
                       .limit(N_STATUS_UPDATES).select("order_id").collect()]
is_upd = F.col("order_id").isin(delivered_ids)

orders_updated = orders.select(
    "order_id", "customer_id",
    F.when(is_upd, F.lit("delivered")).otherwise(F.col("order_status")).alias("order_status"),
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    F.when(is_upd, F.lit(now_s)).otherwise(F.col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
    "order_estimated_delivery_date",
)
write_csv(orders_updated.unionByName(new_orders).select(orders.columns), "olist_orders_dataset.csv")

# COMMAND ----------

# MAGIC %md ## Order items for the new orders

# COMMAND ----------

items = read_src("olist_order_items_dataset.csv")
new_items = items.filter(F.col("order_id").isin(template_ids)).select(
    new_id.alias("order_id"), "order_item_id", "product_id", "seller_id",
    "shipping_limit_date", "price", "freight_value",
)
write_csv(items.unionByName(new_items).select(items.columns), "olist_order_items_dataset.csv")

# COMMAND ----------

print(f"""New batch ready: {dst}
  customers moved:          {len(mover_ids):,}
  new orders:               {len(template_ids):,}  ({len(mover_orders):,} from customers who moved)
  new order items:          {new_items.count():,}
  shipped → delivered:      {len(delivered_ids):,}

Next: run the retail_lakehouse_pipeline job (gold task in incremental mode).""")
