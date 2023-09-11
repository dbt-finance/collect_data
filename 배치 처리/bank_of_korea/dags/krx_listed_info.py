from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from datetime import datetime, timedelta
import json

schema = 'ex_raw_data'
table = 'krx_listed_info'
s3_bucket = 'de-1-2-dokyo'
# default_args = {
#     "retries" : 3,
#     "retry_delay": timedelta(minutes=3)
# }

@task
def specify_partition(**context):
    day_minus_one = context['execution_date'] - timedelta(days=1)
    hook = PostgresHook(postgres_conn_id="redshift_default")
    sql = f"""
        alter table {schema}.{table} add if not exists
        partition(dt='{day_minus_one.strftime("%Y-%m")}')
        location 's3://de-1-2-dokyo/data/securities/krx_listed_info/dt={day_minus_one.strftime("%Y-%m")}/'
        ;"""
    hook.run(sql, True)

with DAG(
    dag_id=table,
    start_date=datetime(2020, 1, 1),
    schedule='0 1 * * *',
    catchup=True,
    max_active_runs=1,
    # default_args=default_args
) as dag:
    lambda_invoke = LambdaInvokeFunctionOperator(
        task_id='lambda_invoke',
        function_name='krx_lambda',
        # not day minus
        payload=json.dumps({
            "s3_bucket": s3_bucket,
            "table": table,
            "service_key": Variable.get("krx_secret_key"),
            "day": "{{ execution_date.strftime('%Y%m%d') }}"
            }
        ),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        dag=dag
    )

lambda_invoke >> specify_partition()




