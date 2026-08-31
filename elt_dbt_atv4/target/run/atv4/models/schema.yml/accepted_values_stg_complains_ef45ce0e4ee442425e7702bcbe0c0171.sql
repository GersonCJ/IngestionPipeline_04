select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        tipo as value_field,
        count(*) as n_records

    from "atv4"."delivery_atv4"."stg_complains"
    group by tipo

)

select *
from all_values
where value_field not in (
    'Conglomerado','Banco/financeira'
)



      
    ) dbt_internal_test