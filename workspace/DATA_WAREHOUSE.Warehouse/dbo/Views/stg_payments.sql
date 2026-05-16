-- Auto Generated (Do not modify) 40AD2C360C9658A3A78A7ED8F72E626011AA9DE1B7A260097FFE8E773BE0B45B
create view [dbo].[stg_payments] as with source as (
    select * from [DATA_WAREHOUSE].[dbo].[raw_payments]

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed;