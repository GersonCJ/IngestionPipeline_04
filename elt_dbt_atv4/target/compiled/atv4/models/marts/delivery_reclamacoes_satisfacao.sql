

select
    c.ano,
    c.trimestre,
    c.categoria,
    c.tipo,
    c.instituicao_financeira,
    b.segmento,
    b.cnpj_base,
    c.indice,
    c.qtd_recl_total,
    c.qtd_recl_reguladas_procedentes,
    c.qtd_clientes_ccs_scr,
    e.employer_name,
    e.nota_geral,
    e.nota_qualidade_vida,
    e.nota_remuneracao_beneficios,
    e.pct_recomendam,
    e.match_percent
from "atv4"."delivery_atv4"."stg_complains" c
left join "atv4"."delivery_atv4"."stg_empregados" e on c.chave_nome = e.chave_nome
left join "atv4"."delivery_atv4"."stg_bancos"     b on c.chave_nome = b.chave_nome
                                       and b.tipo_registro = 'INSTITUICAO'