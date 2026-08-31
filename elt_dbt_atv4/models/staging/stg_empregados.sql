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
        {{ normaliza_nome('nome_instituicao') }} as chave_nome,
        row_number() over (partition by {{ normaliza_nome('nome_instituicao') }}
                           order by match_percent desc, employer_name) as rn
    from {{ source('trusted', 'employer_segments') }}
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
