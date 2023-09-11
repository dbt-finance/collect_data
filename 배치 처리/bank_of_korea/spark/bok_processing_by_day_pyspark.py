from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('bok_process_by_day').enableHiveSupport().getOrCreate()

s3_bucket_path = 's3://de-1-2-dokyo/data/korean_bank/'

spark.read.parquet(f'{s3_bucket_path}base_interest_rate/').createOrReplaceTempView('base_interest_rate')
spark.read.parquet(f'{s3_bucket_path}EUR_to_KRW_exchange_rate/').createOrReplaceTempView('eur_to_krw_exchange_rate')
spark.read.parquet(f'{s3_bucket_path}JPY_to_KRW_exchange_rate/').createOrReplaceTempView('jpy_to_krw_exchange_rate')
spark.read.parquet(f'{s3_bucket_path}USD_to_KRW_exchange_rate/').createOrReplaceTempView('usd_to_krw_exchange_rate')
spark.read.parquet(f'{s3_bucket_path}KOSPI_index/').createOrReplaceTempView('kospi_index')
spark.read.parquet(f'{s3_bucket_path}KOSPI_trading_volume/').createOrReplaceTempView('kospi_trading_volume')
spark.read.parquet(f'{s3_bucket_path}KOSDAQ_index/').createOrReplaceTempView('kosdaq_index')
spark.read.parquet(f'{s3_bucket_path}KOSDAQ_trading_volume/').createOrReplaceTempView('kosdaq_trading_volume')

# df= spark.sql("SELECT to_date(bi.TIME, 'yyyyMMdd') date, bi.DATA_VALUE from base_interest_rate bi")
df = spark.sql("""
SELECT
    to_date(bi.TIME, 'yyyyMMdd') date,
    bi.DATA_VALUE base_interest,
    eur.DATA_VALUE eur,
    jpy.DATA_VALUE jpy,
    usd.DATA_VALUE usd,
    kospii.DATA_VALUE kospi_index,
    kospiv.DATA_VALUE kospi_volume,
    kosdaqi.DATA_VALUE kosdaq_index,
    kosdaqv.DATA_VALUE kosdaq_volume
from base_interest_rate bi
LEFT JOIN eur_to_krw_exchange_rate eur on bi.time = eur.time
LEFT JOIN jpy_to_krw_exchange_rate jpy on bi.time = jpy.time
LEFT JOIN usd_to_krw_exchange_rate usd on bi.time = usd.time
LEFT JOIN kospi_index kospii on bi.time =kospii.time
LEFT JOIN kospi_trading_volume kospiv on bi.time = kospiv.time
LEFT JOIN kosdaq_index kosdaqi on bi.time = kosdaqi.time
LEFT JOIN kosdaq_trading_volume kosdaqv on bi.time = kosdaqv.time
order by bi.time desc
"""
)

df.write.mode('overwrite').parquet(f'{s3_bucket_path}bok_day_agg/')
