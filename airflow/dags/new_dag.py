import airflowlib.emr_util as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2020,7, 22),
    'retries':0,
    'retry_delay': timedelta(minutes=2),
    'provide_context':True
}

dag = DAG(
    dag_id = 'transform_blog_data',
    concurrency=3,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)

region='us-east-2'#emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
# def create_emr(**kwargs):
#     cluster_id = emr.create_cluster(region_name=region,cluster_name='bloggers_cluster', num_core_nodes=2)
#     return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
# def wait_for_completion(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
# def terminate_emr(**kwargs):
#     ti = kwargs['ti']
#     cluster_id = ti.xcom_pull(task_ids='create_cluster')
#     emr.terminate_cluster(cluster_id)


# Converts each of the movielens datafile to parquet
def transform_authors_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    #cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = 'ec2-3-134-93-54.us-east-2.compute.amazonaws.com'#emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/authors.py')
    #emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

def transform_comments_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    #cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = 'ec2-3-134-93-54.us-east-2.compute.amazonaws.com' #emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/comments.py')
    #emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


def transform_posts_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    #cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = 'ec2-3-134-93-54.us-east-2.compute.amazonaws.com' #emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/posts.py')
    #emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

# Define the individual tasks using Python Operators
# create_cluster = PythonOperator(
#     task_id='create_cluster',
#     python_callable=create_emr,
#     dag=dag)

# wait_for_cluster_completion = PythonOperator(
#     task_id='wait_for_cluster_completion',
#     python_callable=wait_for_completion,
#     dag=dag)

transform_authors = PythonOperator(
    task_id='transform_authors',
    python_callable=transform_authors_to_parquet,
    dag=dag)

transform_comments = PythonOperator(
    task_id='transform_comments',
    python_callable=transform_comments_to_parquet,
    dag=dag)

transform_posts = PythonOperator(
    task_id='transform_posts',
    python_callable=transform_posts_to_parquet,
    dag=dag)


# terminate_cluster = PythonOperator(
#     task_id='terminate_cluster',
#     python_callable=terminate_emr,
#     trigger_rule='all_done',
#     dag=dag)

# construct the DAG by setting the dependencies
#create_cluster >> wait_for_cluster_completion
#wait_for_cluster_completion >> transform_authors >> terminate_cluster
#wait_for_cluster_completion >> transform_comments >> terminate_cluster
#wait_for_cluster_completion >> transform_posts >> terminate_cluster

transform_authors >> transform_comments
transform_comments >> transform_posts
