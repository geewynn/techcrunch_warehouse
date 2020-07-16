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