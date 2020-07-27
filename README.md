# TechCrunch Data Warehouse and pipeline

# Project Description
This project involves building a data warehouse with an airflow pipeline. The data involved was gotten from kaggle https://www.kaggle.com/lakritidis/identifying-influential-bloggers-techcrunch/data?select=authors.csv. The data model was built with business users in mind and on hwo they can derive meaningful insights from the data such as comparing sentiments on a post, blogger with the most influence etc.
Built a sentiment analysis model to classify blog comments as positive and negative

# Data Model
The data was organised into a star schema dimensional model. The Star schema model consist of 3 dimensional tables and 2 fact tables

### Dimension Tables 
- 1. Author - author_id, author, meibi, meibx
- 2. Comments - comment_id, post_id, content, author, date, vote
- 3. posts - post_id, title, blogger_name, blogger_id, number_of_comments, content, url, date, number_of_retrieved_comments
### Fact Tables
- 1. word cout - author_id, author, word_count_stopwords, word_count_nostopwords
- 2. comment_review - author_id, author, post_id, comment_id, date, content, sentiment.



# Getting Started

## Airflow
Before starting the dag, you need to set redshift and s3 connections. Also, you will need to set airflow variables.
This configuration can be done on airflow UI by navigating to the connections and variables section under the admin tab.

## Redshift
The airflow ETL needs the table to be ready before running the full pipeline. 
- Run create_cluster.sh to create a redshift cluster.
- Create the tables by running create_tables.py

The configuration settings and files can be found in the export_env_variables.sh file.

## EMR
EMR cluster is needed for running the transformation files. The pyspark script can be found in the transfom folder. The emr_util file contains functions for creating cluster, starting a spark session and terminating a spark session.

The EMR tasks are included in the dag file.
