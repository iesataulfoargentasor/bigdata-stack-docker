-- Sólo cuentan como venta los pedidos cobrados
with pedidos as (
    select order_id, customer_id
    from {{ ref('stg_orders') }}
    where order_status in ('COMPLETE', 'CLOSED')
)

select
    c.customer_city as ciudad,
    c.customer_state as provincia,
    count(distinct p.order_id) as num_pedidos,
    count(distinct p.customer_id) as num_clientes,
    sum(i.subtotal) as importe_total,
    round(sum(i.subtotal) / count(distinct p.order_id), 2) as ticket_medio
from pedidos as p
join {{ ref('stg_customers') }} as c on p.customer_id = c.customer_id
join {{ ref('stg_order_items') }} as i on p.order_id = i.order_id
group by 1, 2
order by importe_total desc
