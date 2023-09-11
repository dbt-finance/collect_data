select cast((di.time || '01') as date),
di.date_value deposit_interest,
li.date_value loan_interest,
da.date_value deoposit_volume,
la.date_value loan_volume
from raw_data.base_deposit_interest_rate di
LEFT JOIN raw_data.base_loan_interest_rate li on di.time = li.time
LEFT JOIN raw_data.deposit_amount da on di.time = da.time
LEFT JOIN raw_data.loan_amount la on di.time = la.time
order by 1 asc
