{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='sync_all_columns'
    )
}}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

paid_orders as (
    select * from {{ ref('int_paid_orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

employees as (
    select * from {{ ref('employees') }}
),

customer_orders as (
    select  customers.customer_id
            , customers.customer_first_name
            , customers.customer_last_name
            , min(order_placed_at) as first_order_date
            , max(order_placed_at) as most_recent_order_date
            , count(orders.order_id) as number_of_orders
    from customers
    left join orders on orders.customer_id = customers.customer_id
    group by 1, 2, 3
),

final as (
    select  paid_orders.customer_id
            , paid_orders.order_id
            , paid_orders.order_placed_at
            , paid_orders.order_status
            , paid_orders.total_amount_paid
            , paid_orders.payment_finalized_date
            
            , customer_orders.customer_first_name
            , customer_orders.customer_last_name
            , row_number() over (order by paid_orders.order_id) as transaction_seq
            , row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq
            , case
                when paid_orders.order_placed_at = customer_orders.first_order_date
                    then 'new'
                else 'return'
              end as nvsr
            , sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.payment_finalized_date) as customer_lifetime_value
            , customer_orders.first_order_date as fdos
            , employees.employee_id 
    from paid_orders
    left join customer_orders on paid_orders.customer_id = customer_orders.customer_id
    left join employees on paid_orders.customer_id = employees.customer_id
)

select * from final

{% if is_incremental() %}
where 
order_placed_at >= (select dateadd('day', -3, max(order_placed_at)) from {{ this }})
{% endif %} 