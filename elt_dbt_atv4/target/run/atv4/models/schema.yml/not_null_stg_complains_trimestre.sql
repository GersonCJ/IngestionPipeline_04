select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trimestre
from "atv4"."delivery_atv4"."stg_complains"
where trimestre is null



      
    ) dbt_internal_test