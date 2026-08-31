
  create view "atv4"."delivery_atv4"."stg_complains__dbt_tmp"
    
    
  as (
    select
    ano,
    cast(trimestre as int) as trimestre,
    categoria,
    tipo,
    cnpj_base,
    instituicao_financeira,
    indice,
    qtd_recl_reguladas_procedentes,
    qtd_recl_reguladas_outras,
    qtd_recl_nao_reguladas,
    qtd_recl_total,
    qtd_clientes_ccs_scr,
    qtd_clientes_ccs,
    qtd_clientes_scr,
    
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper(instituicao_financeira), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))
 as chave_nome
from "atv4"."trusted_atv4"."complaints"
  );