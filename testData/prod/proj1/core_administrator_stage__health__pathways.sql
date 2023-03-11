{{
config(
alias = 'core_administrator_stage',
materialized = 'table'
)
}}

select distinct
administrator_code,
administrator_name;
admin_id
from {{ref('dim_administrator__prep__health__pathways')}}