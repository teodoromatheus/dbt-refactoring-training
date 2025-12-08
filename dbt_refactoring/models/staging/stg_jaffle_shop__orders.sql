with source as (
    select * from {{ source('jaffle_shop','orders') }}
),

converted as (
    select  ID as order_id
            , ORDER_DATE as order_placed_at
            , STATUS as order_status
            , USER_ID as customer_id
    from source
)

select * from converted