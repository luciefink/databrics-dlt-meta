# databricks-dlt-meta

### Test Project for DLT-META Library

_Create pipelines to load data into bronze and silver layers using the dlt-meta library to automate declarative pipeline configuration_

**Sources:**
- https://databrickslabs.github.io/dlt-meta/index.html
- https://github.com/databrickslabs/dlt-meta 

---

## Prerequisites/Notes

In Databricks, use a catalog/schema of your preference. In this project, Catalog: `lucie_finkova` and Schema: `dlt_meta_lukas` were used.

Use the `src/onboarding/schema_setting.ipynb` file for schema and volume creation.

DBFS has limited access in **Databricks Free Edition**, so input files were loaded to Volumes instead. Autoloader (cloudFiles) is used as the reader option.

---

## Instructions

### 1. Prepare Input Files

This project uses 3 input files: **Customers** (CSV), **Orders** (JSON), and **Products** (CSV).  
**Repository link:** https://github.com/luciefink/databrics-dlt-meta/tree/main/data

Load files to the respective folders in `/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_data`.  
**Note:** Initially load only the first set of files; keep the second set for an additional pipeline run.

### 2. Load DDL Files for Input Files

Load DDL files from the repository (https://github.com/luciefink/databrics-dlt-meta/tree/main/ddl) to `/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_ddl`.

### 3. Configure Settings with Config Files

**onboarding.json**  
This is the main configuration file where you set:
- Names and paths of input files
- Reading options
- Table names
- Quarantine table configuration
- SCD (Slowly Changing Dimensions) settings
- Partitions and clusters
- Path to silver transformations
- Path to DQE (Data Quality Expectations) files

**silver_transformations.json**  
This file defines the SELECT statements for target silver tables.

**DQE Folder**  
Contains data quality definitions for both bronze and silver data sets (customers, products, orders).

**Important:** When the pipeline runs, the `expect_or_quarantine` condition is applied first (if configured), followed by `expect_or_drop`. Always define `expect_or_drop`, or the flow will not be created!

Load all config files from the repository (https://github.com/luciefink/databrics-dlt-meta/tree/main/config) with the same folder structure to `/Volumes/lucie_finkova/dlt_meta_lukas/dlt_meta_conf`.

### 4. Prepare and Run the Onboarding File

Create a notebook for onboarding config files (repository: https://github.com/luciefink/databrics-dlt-meta/blob/main/src/onboarding/onboarding.ipynb) and run it.

This onboarding script will create Dataflowspec tables that guide the pipeline:
- `lucie_finkova.dlt_meta_lukas.bronze_dataflowspec_table`
- `lucie_finkova.dlt_meta_lukas.silver_dataflowspec_table`

**Note:** This script uses DLT-META from a cloned GitHub repository instead of pip install due to Databricks Free Edition limitations. The library is installed but cannot be used/found through standard pip.

### 5. Prepare the Pipeline File

Create a Python file as the main pipeline file. One file is used for both bronze and silver pipelines with different pipeline settings.  
**Repository:** https://github.com/luciefink/databrics-dlt-meta/blob/main/src/pipeline/dlt_meta_pipeline.py

### 6. Create the Pipeline

In the Databricks UI, create a new pipeline and set the configuration. See the JSON file for reference (repository: https://github.com/luciefink/databrics-dlt-meta/blob/main/src/pipeline/sample_pipeline_setting.json).

Create two separate pipelines:
- One for bronze layer loading
- One for silver layer loading

### 7. Create a Job to Orchestrate Bronze and Silver Pipelines

Create a Databricks Job to orchestrate the bronze and silver pipelines in sequence.

---