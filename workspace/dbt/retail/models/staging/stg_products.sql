select
    p.product_id,
    p.product_name,
    cast(p.product_price as decimal(10, 2)) as product_price,
    c.category_name,
    d.department_name
from {{ source('retail_raw', 'products') }} as p
join {{ source('retail_raw', 'categories') }} as c
    on p.product_category_id = c.category_id
join {{ source('retail_raw', 'departments') }} as d
    on c.category_department_id = d.department_id
