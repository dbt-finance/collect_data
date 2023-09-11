SELECT
    cast(bi.time as date),
    bi.date_value base_interest,
    eur.date_value eur,
    jpy.date_value jpy,
    usd.date_value usd,
    kospii.date_value kospi_index,
    kospiv.date_value kospi_volume,
    kosdaqi.date_value kosdaq_index,
    kosdaqv.date_value kosdaq_volume
from raw_data.base_interest_rate bi
LEFT JOIN raw_data.eur_to_krw_exchange_rate eur on bi.time = eur.time
LEFT JOIN raw_data.jpy_to_krw_exchange_rate jpy on bi.time = jpy.time
LEFT JOIN raw_data.usd_to_krw_exchange_rate usd on bi.time = usd.time
LEFT JOIN raw_data.kospi_index kospii on bi.time =kospii.time
LEFT JOIN raw_data.kospi_trading_volume kospiv on bi.time = kospiv.time
LEFT JOIN raw_data.kosdaq_index kosdaqi on bi.time = kosdaqi.time
LEFT JOIN raw_data.kosdaq_trading_volume kosdaqv on bi.time = kosdaqv.time
order by bi.time desc