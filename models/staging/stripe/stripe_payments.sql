WITH source as (

    select *
    from {{ source('stripe', 'payment') }}
),
transformed_payments AS (

        select id AS payments_id,
        status AS payment_status,
        round(amount/100.0,2) as payment_amount,
        orderid AS order_id
        from source
)

select * from transformed_payments