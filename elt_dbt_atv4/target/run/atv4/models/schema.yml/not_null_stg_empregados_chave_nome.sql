select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select chave_nome
from "atv4"."delivery_atv4"."stg_empregados"
where chave_nome is null



      
    ) dbt_internal_test