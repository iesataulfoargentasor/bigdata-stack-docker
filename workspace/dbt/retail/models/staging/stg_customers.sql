select
    customer_id,
    customer_fname || ' ' || customer_lname as customer_name,
    customer_email,
    customer_city,
    customer_state,
    customer_zipcode
from {{ source('retail_raw', 'customers') }}
