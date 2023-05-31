with 

source as (

    select * from {{ source('dbt_dmenteshashvili', 'payments') }}

),

transformed as (

  select

    id as payment_id,
    orderid as order_id,
    created as payment_created_at,
    status as payment_status,
    {{ cents_to_dollars('amount', 4) }} as payment_amount

  from source

)

select * from transformed

-- {{ limit_data_in_dev('payment_created_at', 3) }}