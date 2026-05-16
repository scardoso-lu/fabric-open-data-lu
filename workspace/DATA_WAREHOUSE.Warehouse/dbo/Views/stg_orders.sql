-- Auto Generated (Do not modify) 83524F21A9D2A40FD90B474A4F3B389A0F8DEAF948175C81742327130D69BE30
create view [dbo].[stg_orders] as with source as (
    select * from [DATA_WAREHOUSE].[dbo].[raw_orders]

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

)

select * from renamed;