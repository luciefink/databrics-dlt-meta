## Pipeline with enriched silver table using data from 2 different bronze tables
- use orders table enriched with customers data
- usage of silver_custom_transform_func argument in invoking pipeline
- BUT before we have to onboard orders input for second time (with different name) to be used in our join  (see dlt-meta source code in dataflow_pipeline.py- _launch_dlt_flow)

- onboardig done with two onboarding jsons - just to clear distinct 3 main tables and 1 duplicated orders
 
       https://github.com/luciefink/databrics-dlt-meta/blob/main/config/onboarding6.json
 
       https://github.com/luciefink/databrics-dlt-meta/blob/main/config/onboarding6_joined.json
  
- also invoking pipeline script is different from previous as we use silver_custom_transform_func
-    
      https://github.com/luciefink/databrics-dlt-meta/blob/main/src/pipeline/dlt_meta_pipe_join.py
  
## Picture of pipeline

<img width="1051" height="745" alt="image" src="https://github.com/user-attachments/assets/f90d0b87-6c60-4d27-ac36-ffea8bf59140" />

