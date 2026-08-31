
  create view "atv4"."delivery_atv4"."stg_bancos__dbt_tmp"
    
    
  as (
    select
    segmento,
    cnpj_base,
    nome_instituicao,
    tipo_registro,
    
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper(nome_instituicao), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))
 as chave_nome
from "atv4"."trusted_atv4"."bancos"
  );