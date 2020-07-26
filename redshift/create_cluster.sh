#!/bin/bash +xe

source export_env_variables.sh

aws redshift create-cluster \
--cluster-identifier blog-cluster \
--db-name dev \
--cluster-type single-node \
--master-username $USERNAME \
--master-user-password $PASSWORD \
--node-type dc2.large \
--vpc-security-group-ids $SECURITY_GROUP \
--iam-roles $IAM_ROLE