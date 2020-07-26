# TechCrunch Data Warehouse and pipeline
## Workflow


# Redshift
The airflow ETL needs the table to be ready before running the full pipeline. 
- Run create_cluster.sh to create a redshift cluster.
- Create the tables by running create_tables.py

The configuration settings and files can be found in the export_env_variables.sh file.

# EMR
EMR cluster is needed for running the transformation files. The pyspark script can be found in the transfom folder. The emr_util file contains functions for creating cluster, starting a spark session and terminating a spark session.

The EMR tasks are included in the dag file.



### Redshift Configuration
- create_cluster.sh contains command to create the redshift cluster.
  ```
    #!/bin/bash +xe
    source export_env_variables.sh

    aws redshift create-cluster \
    --cluster-identifier redhsift-cluster \
    --db-name dev
    --cluster-type single-node \
    --master-user-username $USERNAME \
    --mater-user-password $PASSWORD \
    --node-type dc2.large \
    --vpc-security-group-ids $SECURITY_GROUP \
    --iam-roles $IAM_ROLE
  ```
 - config.cfg contains the configuration details for our AWS and redshift control
  ```
    [CLUSTER]
      HOST=<REDSHIFT_HOST>
      DB_NAME=<REDSHIFT_DB_NAME>
      DB_USER=<REDSHIFT_DB_USER>
      DB_PASSWORD=<REDSHIFT_DB_PASSWORD>
      DB_PORT=<REDSHIFT_DB_PORT>

      [AWS]
      AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
      AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
      AWS_IAM_ROLE=<AWS_IAM_ROLE>

      [DATA_S3]
      ETL_TEMP_S3_BUCKET=<DATA_S3>
 ```
 - export_env_variables.sh contain environment variables
 ```
      export USERNAME=<REDSHIFT_USERNAME>
      export PASSWORD=<REDSHIFT_PASSWORD>
      export SECURITY_GROUP=<REDSHIFT_SECURITY_GROUP>
      export IAM_ROLE=<REDSHIFT_IAM_ROLE>
 ```
 ### Create Tables
 creating the tables requires to 2 files, the create_tables.sql and create_tables.py
 - The create_tables.sql - this file holds all the create tables sql statement for creating tables in our redshift data warehouse.
 - The create_tables.py connects to the database and run the create_tables.sql and creates the seperate tables.
 
 ### Transformation Files
Python script in the transformation folder are used to transform the data using spark, the transformed data are then written to a temporary folder in AWS S3 in parquet format.

 
 
My workflow
create_tables.ql ---> create_tables.py ---> export_env_variables.sh ---> create_cluster.sh ---> config.cfg
