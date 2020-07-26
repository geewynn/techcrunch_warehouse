# TechCrunch Data Warehouse and pipeline
## Workflow


## Redshift
The airflow ETL needs the table to be ready before running the full pipeline. 
- Run create_cluster.sh to create a redshift cluster.
- Create the tables by running create_tables.py

The configuration settings and files can be found in the export_env_variables.sh file.

## EMR
EMR cluster is needed for running the transformation files. The pyspark script can be found in the transfom folder. The emr_util file contains functions for creating cluster, starting a spark session and terminating a spark session.

The EMR tasks are included in the dag file.

 
 
My workflow
create_tables.ql ---> create_tables.py ---> export_env_variables.sh ---> create_cluster.sh ---> config.cfg
