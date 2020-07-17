 import airflowlib.emr_util as emr
from techcrunch_subdag import subdag

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


DAG_NAME = 'yelp_analysis_dag'
REDSHIFT_CONN_ID ='redshift'
S3_CONN_ID ='s3'


start_date = airflow.utils.dates.days_ago(51)
default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'catchup' : True,
    'provide_context': True,
    'wait_for_downstream' : True
}


dag = DAG(
    dag_id = DAG_NAME,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily'
)


def clean_temp_s3(**kwargs):
    """Clean S3 bucket where temp files will be saved"""
    s3_conn = S3Hook(S3_CONN_ID)

    temp_keys = s3_conn.list_keys(Variable.get('TEMP_FILE_BUCKET'), prefix='temp')
    print("Delete temp S3 files: " + str(temp_keys))

    if(temp_keys):
        S3Hook(S3_CONN_ID).delete_objects(
            bucket=Variable.get('TEMP_FILE_BUCKET'),
            keys=temp_keys)


start_operator = DummyOperator(
    task_id='begin_execution', 
    dag=dag
)


task_transform_author_to_redshift = SubDagOperator(
    task_id='task_transform_author_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_author_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='authors',
        table_names=['authors', 'word_count']),
    dag=dag
)

task_transform_comment_to_redshift = SubDagOperator(
    task_id='task_transform_comment_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_checkin_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='comments',
        table_names=['comments', 'comment_review']),
    dag=dag
)

task_transform_post_to_redshift = SubDagOperator(
    task_id='task_transform_post_to_redshift',
    subdag=subdag(
        parent_dag_name=DAG_NAME,
        child_dag_name='task_transform_post_to_redshift',
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        script_name='posts',
        table_names=['posts']),
    dag=dag
)

tasks_transform_to_redshift = [
    task_transform_author_to_redshift,
    task_transform_comment_to_redshift,
    task_transform_post_to_redshift,
]

task_clean_temp_s3 = PythonOperator(
    task_id='task_clean_temp_s3',
    dag=dag,
    python_callable=clean_temp_s3
)

end_operator = DummyOperator(
    task_id='stop_execution',
    dag=dag,
    trigger_rule='none_failed'
)

### Airflow task dependencies
start_operator >> task_clean_temp_s3
task_clean_temp_s3 >> tasks_transform_to_redshift
tasks_transform_to_redshift >> end_operator
