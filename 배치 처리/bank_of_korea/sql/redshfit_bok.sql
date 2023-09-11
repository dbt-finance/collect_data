create table analytics.bok_day_agg
(
    time          date encode az64,
    base_interest double precision,
    eur           double precision,
    jpy           double precision,
    usd           double precision,
    kospi_index   double precision,
    kospi_volume  double precision,
    kosdaq_index  double precision,
    kosdaq_volume double precision
)