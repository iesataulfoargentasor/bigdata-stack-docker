select
    pr.department_name as departamento,
    pr.category_name as categoria,
    sum(i.quantity) as unidades,
    sum(i.subtotal) as importe_total
from {{ ref('stg_order_items') }} as i
join {{ ref('stg_orders') }} as o on i.order_id = o.order_id
join {{ ref('stg_products') }} as pr on i.product_id = pr.product_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by 1, 2
order by 1, importe_total desc
