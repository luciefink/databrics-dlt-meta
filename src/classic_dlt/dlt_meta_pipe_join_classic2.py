from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

'''
For all bronze tables we quarantine invalid records, they are separated from main bronze tables. Raw data is loaded to temporary bronze tables and from 
here we split invalid records to quarantine and valid to regular bronze table. 
'''
CATALOG = "workspace"
SCHEMA = "default"


def full_table(table_name):
    return f"{CATALOG}.{SCHEMA}.{table_name}"
# ============================================================
# CUSTOMERS BRONZE
# ============================================================

customers_rules = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_age": "age IS NULL OR (age >= 0 AND age <= 120)",
    "valid_email": "email IS NULL OR email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"
}
customers_quarantine_rule = "NOT({0})".format(" AND ".join(customers_rules.values()))

@dp.table(temporary=True, partition_cols=["is_quarantined"])
@dp.expect_all(customers_rules)
def customers_bronze_temp():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load("/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_data/customers")
        .withColumn("is_quarantined", F.expr(customers_quarantine_rule))
    )

@dp.table(name="customers_bronze6", comment="customers bronze table6", cluster_by=["country"])
def customers_bronze6():
    return (
        spark.readStream.table("customers_bronze_temp")
        .filter("is_quarantined = false")
        .drop("is_quarantined")
    )

@dp.table(name="customers_quarantine6", comment="customers quarantine table6", cluster_by=["country"])
def customers_quarantine6():
    return (
        spark.readStream.table("customers_bronze_temp")
        .filter("is_quarantined = true")
        .drop("is_quarantined")
    )

# ============================================================
# ORDERS BRONZE
# ============================================================

orders_rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_order_amount": "order_amount IS NULL OR order_amount >= 0",
    "valid_order_date": "order_date IS NOT NULL",
    "valid_order_status": "order_status IN ('pending','processing','shipped','delivered','cancelled')"
}
orders_quarantine_rule = "NOT({0})".format(" AND ".join(orders_rules.values()))

@dp.table(temporary=True, partition_cols=["is_quarantined"])
@dp.expect_all(orders_rules)
def orders_bronze_temp():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_data/orders")
        .withColumn("is_quarantined", F.expr(orders_quarantine_rule))
    )

@dp.table(name="orders_bronze6", comment="orders bronze table6", cluster_by=["payment_method"])
def orders_bronze6():
    return (
        spark.readStream.table("orders_bronze_temp")
        .filter("is_quarantined = false")
        .drop("is_quarantined")
    )

@dp.table(name="orders_quarantine6", comment="orders quarantine table6", cluster_by=["payment_method"])
def orders_quarantine6():
    return (
        spark.readStream.table("orders_bronze_temp")
        .filter("is_quarantined = true")
        .drop("is_quarantined")
    )

# ============================================================
# PRODUCTS BRONZE
# ============================================================

products_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "no_rescued_data": "_rescued_data IS NULL",
    "valid_price": "price IS NULL OR (price >= 0 AND price <= 50000)",
    "valid_stock_quantity": "stock_quantity IS NULL OR stock_quantity >= 0"
}
products_quarantine_rule = "NOT({0})".format(" AND ".join(products_rules.values()))

@dp.table(temporary=True, partition_cols=["is_quarantined"])
@dp.expect_all(products_rules)
def products_bronze_temp():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("header", "true")
        .load("/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_data/products")
        .withColumn("is_quarantined", F.expr(products_quarantine_rule))
    )

@dp.table(name="products_bronze6", comment="products bronze table6", cluster_by=["product_id"])
def products_bronze6():
    return (
        spark.readStream.table("products_bronze_temp")
        .filter("is_quarantined = false")
        .drop("is_quarantined")
    )

@dp.table(name="products_quarantine6", comment="products quarantine table6", cluster_by=["product_id"])
def products_quarantine6():
    return (
        spark.readStream.table("products_bronze_temp")
        .filter("is_quarantined = true")
        .drop("is_quarantined")
    )

# ============================================================
# SILVER — CUSTOMERS (SCD2)
# ============================================================

dp.create_streaming_table(
    name="customers_silver6",
    comment="customers silver table6",
    cluster_by=["country"]
)

dp.create_auto_cdc_flow(
    target="customers_silver6",
    source="customers_bronze6",
    keys=["customer_id"],
    sequence_by="updated_timestamp",
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("state = 'D'"),
    except_column_list=["state", "updated_timestamp", "_rescued_data"]
)

# ============================================================
# SILVER — ORDERS (1:1)
# ============================================================

@dp.table(
    name="orders_silver6",
    comment="orders silver table6",
    cluster_by=["payment_method"]
)
def orders_silver6():
    return (
        spark.readStream.table("orders_bronze6")
        .select(
            "order_id", "customer_id", "order_date", "order_amount",
            "order_status", "payment_method", "shipping_address",
            "updated_at", "_rescued_data"
        )
    )

# ============================================================
# SILVER — PRODUCTS (SCD2)
# ============================================================

dp.create_streaming_table(
    name="products_silver6",
    comment="products silver table6",
    cluster_by=["product_id"]
)

dp.create_auto_cdc_flow(
    target="products_silver6",
    source="products_bronze6",
    keys=["product_id"],
    sequence_by="created_timestamp",
    stored_as_scd_type=2,
    apply_as_deletes=F.expr("state = 'D'"),
    except_column_list=["state", "created_timestamp", "_rescued_data"]
)

# ============================================================
# SILVER — CUSTOMERS_ORDERS ENRICHED JOIN
# ============================================================

@dp.table(
    name="customers_orders_silver6",
    comment="orders enriched with customer data"
)
def customers_orders_silver6():
    valid_customers_filter = " AND ".join(customers_rules.values())

    # static read 
    customers_df = (
        spark.read.table(full_table("customers_bronze6"))
        .filter(valid_customers_filter)
        .groupBy("customer_id", "first_name", "last_name", "email", "country")
        .agg(F.max("updated_timestamp"))
        .drop("max(updated_timestamp)")
    )

    # streaming source (in basic variant we create final materialized view)
    return (
        spark.readStream.table(full_table("orders_bronze6"))
        .alias("o")
        .join(customers_df.alias("c"), on="customer_id", how="left")
        .select(
            "o.order_id", "o.customer_id", "o.order_date", "o.order_amount",
            "o.order_status", "o.payment_method", "o.shipping_address",
            F.concat(F.col("c.first_name"), F.lit(" "), F.col("c.last_name"))
                .alias("full_name"),
            "c.email", "c.country"
        )
    )
