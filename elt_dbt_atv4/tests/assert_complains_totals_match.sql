-- Fails (returns rows) if procedentes + outras + nao_reguladas != total.
-- Verified to hold for all 918 rows during trusted-layer validation in Python.
select *
from {{ ref('stg_complains') }}
where qtd_recl_reguladas_procedentes
    + qtd_recl_reguladas_outras
    + qtd_recl_nao_reguladas
    != qtd_recl_total
