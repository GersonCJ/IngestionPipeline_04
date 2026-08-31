
  create view "atv4"."delivery_atv4"."stg_empregados__dbt_tmp"
    
    
  as (
    with base as (
    select
        employer_name,
        nome_instituicao,
        segmento,
        nota_geral,
        nota_cultura_valores,
        nota_qualidade_vida,
        nota_remuneracao_beneficios,
        pct_recomendam,
        match_percent,
        
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper(nome_instituicao), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))
 as chave_nome,
        row_number() over (partition by 
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper(nome_instituicao), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))

                           order by match_percent desc, employer_name) as rn
    from "atv4"."trusted_atv4"."employer_segments"
)
select
    employer_name,
    nome_instituicao,
    segmento,
    nota_geral,
    nota_cultura_valores,
    nota_qualidade_vida,
    nota_remuneracao_beneficios,
    pct_recomendam,
    match_percent,
    chave_nome
from base
where rn = 1
  );