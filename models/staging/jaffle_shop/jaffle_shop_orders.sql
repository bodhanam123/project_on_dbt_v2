WITH source as (

    select *
    from {{ source('jaffle_shop', 'orders') }}
),
transformed_orders AS
(
      select 
        id AS order_id,
        order_date,
        status AS order_status,
        user_id AS customer_id,
        row_number() over (partition by user_id order by order_date, id) as user_order_seq
      from source
)

select * from transformed_orders