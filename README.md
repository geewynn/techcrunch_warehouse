# TechCrunch Data Warehouse and pipeline
## Workflow

### Redshift COnfiguration
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
