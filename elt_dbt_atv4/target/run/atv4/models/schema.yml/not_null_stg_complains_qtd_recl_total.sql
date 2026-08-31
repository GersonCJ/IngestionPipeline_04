select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select qtd_recl_total
from "atv4"."delivery_atv4"."stg_complains"
where qtd_recl_total is null



      
    ) dbt_internal_test