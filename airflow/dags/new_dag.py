import airflowlib.emr_util as emr
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

S3_CONN_ID='s3'
redshift_conn_id='redshift'


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2020,7, 26),
    'retries':0,
    'retry_delay': timedelta(minutes=2),
    'provide_context':True
}


def clean_temp_s3(**kwargs):
    """Clean S3 bucket where temp files will be saved"""
    s3_conn = S3Hook(S3_CONN_ID)

    temp_keys = s3_conn.list_keys(Variable.get('TEMP_FILE_BUCKET'), prefix='temp')
    print("Delete temp S3 files: " + str(temp_keys))

    if(temp_keys):
        S3Hook(S3_CONN_ID).delete_objects(
            bucket=Variable.get('TEMP_FILE_BUCKET'),
            keys=temp_keys)

region='us-east-2'#emr.get_region()
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region,cluster_name='bloggers_cluster', num_core_nodes=1)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)


# Converts each of the movielens datafile to parquet
def transform_authors_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/authors.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

def transform_comments_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/comments.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


def transform_posts_to_parquet(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                            '/home/godwin/airflow/blogger/dags/transform/posts.py')
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

def copy_comments_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load comments.parquet to Redshift")
        copy_sql = """
        COPY comments
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/comments.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)

def copy_author_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load comments.parquet to Redshift")
        copy_sql = """
        COPY author
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/author.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)


def copy_posts_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load comments.parquet to Redshift")
        copy_sql = """
        COPY posts
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/posts.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)


def copy_comments_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load comments.parquet to Redshift")
        copy_sql = """
        COPY comments
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/comments.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)


def copy_comment_review_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load comment_review.parquet to Redshift")
        copy_sql = """
        COPY comment_review
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/comment_review.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)


def copy_word_count_to_redshift(**kwargs):
        s3_credential = S3Hook(S3_CONN_ID).get_credentials()

        print(f"Load word_count.parquet to Redshift")
        copy_sql = """
        COPY word_count
        FROM 's3://{PARQUET_FOLDER_NAME}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        """.format(
            PARQUET_FOLDER_NAME=f"{Variable.get('TEMP_FILE_BUCKET')}/temp/word_count.parquet",
            IAM_ROLE=Variable.get('IAM_ROLE')
        )
        PostgresHook(redshift_conn_id).run(copy_sql)


dag = DAG(
    dag_id = 'transform_blog_data',
    concurrency=1,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)



comments_to_redshift = PythonOperator(
    task_id='comments_to_redshift',
    dag=dag,
    python_callable=copy_comments_to_redshift
)

author_to_redshift = PythonOperator(
    task_id='author_to_redshift',
    dag=dag,
    python_callable=copy_author_to_redshift
)

posts_to_redshift = PythonOperator(
    task_id='posts_to_redshift',
    dag=dag,
    python_callable=copy_posts_to_redshift
)

comment_review_to_redshift = PythonOperator(
    task_id='comment_review_to_redshift',
    dag=dag,
    python_callable=copy_comment_review_to_redshift
)

word_count_to_redshift = PythonOperator(
    task_id='word_count_to_redshift',
    dag=dag,
    python_callable=copy_word_count_to_redshift
)


start_operator = DummyOperator(
    task_id='begin_execution', 
    dag=dag
)

task_clean_temp_s3 = PythonOperator(
    task_id='task_clean_temp_s3',
    dag=dag,
    python_callable=clean_temp_s3
)

#Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

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


terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# construct the DAG by setting the dependencies
start_operator >> task_clean_temp_s3
task_clean_temp_s3 >> create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> transform_authors >> terminate_cluster
wait_for_cluster_completion >> transform_comments >> terminate_cluster
wait_for_cluster_completion >> transform_posts >> terminate_cluster


terminate_cluster >> comments_to_redshift
terminate_cluster >> author_to_redshift
terminate_cluster >> posts_to_redshift
terminate_cluster >> comment_review_to_redshift
terminate_cluster >> word_count_to_redshift