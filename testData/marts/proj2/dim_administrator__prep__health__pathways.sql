{{
    config(
        materialized = 'view',
        alias = 'dim_administartor_stage'
    )
}}

with source_data as (
    select *
    from {{source'health__pathways', 'dim_administrator'}}
)

select * from source_data