select
    order_item_id,
    order_item_order_id as order_id,
    order_item_product_id as product_id,
    order_item_quantity as quantity,
    cast(order_item_subtotal as decimal(10, 2)) as subtotal,
    cast(order_item_product_price as decimal(10, 2)) as unit_price
from {{ source('retail_raw', 'order_items') }}
