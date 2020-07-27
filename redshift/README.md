# Redshift
This folder contains all you need to create a redshift cluster and create database tables.

### Redshift Configuration

 - export_env_variables.sh contain environment variables
 ```
      export USERNAME=<REDSHIFT_USERNAME>
      export PASSWORD=<REDSHIFT_PASSWORD>
      export SECURITY_GROUP=<REDSHIFT_SECURITY_GROUP>
      export IAM_ROLE=<REDSHIFT_IAM_ROLE>
 ```

### Create table configuration

  ```
 - config.cfg contains the configuration details for our AWS and redshift control

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
 
