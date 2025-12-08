with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

last_update_orders as (
    select  order_id
            , max(payment_created_at) as payment_finalized_date
            , sum(payment_amount) as total_amount_paid
    from payments
    where payment_status = 'success'
    group by 1
),

final as (
    select  orders.order_id
            , orders.customer_id
            , orders.order_placed_at
            , orders.order_status
            , last_update_orders.total_amount_paid
            , last_update_orders.payment_finalized_date
            , sum(last_update_orders.total_amount_paid) over (partition by orders.customer_id order by last_update_orders.payment_finalized_date) as customer_lifetime_value
    from orders
    left join last_update_orders on orders.order_id = last_update_orders.order_id
)

select * from final