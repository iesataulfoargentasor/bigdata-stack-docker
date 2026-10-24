select
    order_id,
    cast(order_date as date) as order_date,
    order_customer_id as customer_id,
    order_status
from {{ source('retail_raw', 'orders') }}
