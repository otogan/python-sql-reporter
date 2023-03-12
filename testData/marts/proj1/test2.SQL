
-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model_1') }}
--where id = 1

-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model_2') }}
--where id = 1
