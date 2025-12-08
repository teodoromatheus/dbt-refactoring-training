with source as (
    select * from {{ source('stripe','payment') }}
),

converted as (
    select  ID as payment_id
            , CREATED as payment_created_at
            , ORDERID as order_id
            , PAYMENTMETHOD as payment_method
            , STATUS as payment_status
            , (AMOUNT / 100.0) as payment_amount
    from source 
)

select * from converted