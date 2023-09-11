create external table ex_raw_data.krx_listed_info
    (
    basdt varchar(16),
    srtncd varchar(100),
    isincd varchar(100),
    mrktctg varchar(100),
    itmsnm varchar(100),
    crno varchar(100),
    corpnm varchar(100)
    )
    partitioned by (dt varchar(10))
    stored as PARQUET
    location 's3://de-1-2-dokyo/data/securities/krx_listed_info_test';

alter table ex_raw_data.krx_listed_info add if not exists
partition(dt='2023-08')
location 's3://de-1-2-dokyo/data/securities/krx_listed_info/dt=2023-08/'
;