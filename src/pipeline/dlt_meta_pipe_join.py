import os
import sys

# 1. Setup Path to Repo
repo_path = "/Workspace/Users/lupinek.fink@gmail.com/DLT-META"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def silver_custom_transform(input_df: DataFrame, dataflow_spec) -> DataFrame:
    target_table = dataflow_spec.targetDetails.get("table")
    
    if target_table == "customers_orders_silver6":
        
        # deduplicate customers keeping latest record per customer_id
        window = Window.partitionBy("customer_id").orderBy(F.col("updated_timestamp").desc())
        
        customers_df = spark.read.table("lucie_finkova.dlt_meta_lukas.customers_bronze6") \
            .withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .select("customer_id", "first_name", "last_name", "email", "country")
        
        joined_df = input_df.alias("o").join(
            customers_df.alias("c"),
            on="customer_id",
            how="left"
        ).select(
            "o.order_id",
            "o.customer_id",
            "o.order_date",
            "o.order_amount",
            "o.order_status",
            "o.payment_method",
            "o.shipping_address",
            "c.first_name",
            "c.last_name",
            "c.email",
            "c.country"
        )
        return joined_df
    
    return input_df

DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer,
    silver_custom_transform_func=silver_custom_transform
)