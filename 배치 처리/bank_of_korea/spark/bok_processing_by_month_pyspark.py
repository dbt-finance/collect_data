from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('bok_process_by_month').enableHiveSupport().getOrCreate()

s3_bucket_path = 's3://de-1-2-dokyo/data/korean_bank/'

spark.read.parquet(f'{s3_bucket_path}base_deposit_interest_rate/').createOrReplaceTempView('base_deposit_interest_rate')
spark.read.parquet(f'{s3_bucket_path}base_loan_interest_rate/').createOrReplaceTempView('base_loan_interest_rate')
spark.read.parquet(f'{s3_bucket_path}deposit_amount/').createOrReplaceTempView('deposit_amount')
spark.read.parquet(f'{s3_bucket_path}loan_amount/').createOrReplaceTempView('loan_amount')

df = spark.sql("""
SELECT
    cast(concat(di.TIME,'01') as date) date,
    di.DATA_VALUE deposit_interest,
    li.DATA_VALUE loan_interest,
    da.DATA_VALUE deoposit_volume,
    la.DATA_VALUE loan_volume
from base_deposit_interest_rate di
LEFT JOIN base_loan_interest_rate li on di.time = li.time
LEFT JOIN deposit_amount da on di.time = da.time
LEFT JOIN loan_amount la on di.time = la.time
"""
)

df.write.mode('overwrite').parquet(f'{s3_bucket_path}bok_month_agg/')
