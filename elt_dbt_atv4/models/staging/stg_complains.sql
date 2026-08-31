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
    {{ normaliza_nome('instituicao_financeira') }} as chave_nome
from {{ source('trusted', 'complaints') }}
