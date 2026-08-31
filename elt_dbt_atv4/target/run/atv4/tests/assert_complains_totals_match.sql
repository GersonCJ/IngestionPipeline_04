select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Fails (returns rows) if procedentes + outras + nao_reguladas != total.
-- Verified to hold for all 918 rows during trusted-layer validation in Python.
select *
from "atv4"."delivery_atv4"."stg_complains"
where qtd_recl_reguladas_procedentes
    + qtd_recl_reguladas_outras
    + qtd_recl_nao_reguladas
    != qtd_recl_total
      
    ) dbt_internal_test