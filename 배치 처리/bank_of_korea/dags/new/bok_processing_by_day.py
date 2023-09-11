from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime
import os

DAG_ID = os.path.basename(__file__).replace(".py",'')

s3_bucket = 'de-1-2-dokyo'
s3_key ='data/korean_bank/bok_day_agg/'
schema = 'analytics'
table = 'bok_day_agg'

SPARK_STEPS = [
    {
        "Name": "bok_process_by_day",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit",
                     '--master', 'yarn',
                     '--deploy-mode', 'cluster',
                     's3://de-1-2-dokyo/etc_file/spark/bok_processing_by_day_pyspark.py'
                     ]
        }
    }
]

with DAG(
    dag_id=DAG_ID,
    schedule='0 2 * * *',
    start_date=datetime(2023,8,29),
    catchup=False,
    max_active_runs=1,
)as dag:
    step_add = EmrAddStepsOperator(
        task_id="step_add",
        aws_conn_id="aws_default",
        job_flow_id="j-1PKP56IEPMESB",
        steps=SPARK_STEPS
    )

    wait_sensor = EmrStepSensor(
        task_id='spark_job_wait',
        aws_conn_id="aws_defaulte",
        job_flow_id="j-1PKP56IEPMESB",
        step_id="{{ task_instance.xcom_pull(task_ids='step_add', key='return_value')[0] }}"
    )

    s3_to_redshift_copy = S3ToRedshiftOperator(
        task_id='s3_to_redshift_copy',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        schema=schema,
        table=table,
        copy_options=['FORMAT AS PARQUET'],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        method="REPLACE",
        dag=dag
    )

step_add >> wait_sensor >> s3_to_redshift_copy