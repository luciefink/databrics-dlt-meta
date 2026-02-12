import os
import sys

# 1. Setup Path to Repo
repo_path = "/Workspace/Users/lupinek.fink@gmail.com/DLT-META"
if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)