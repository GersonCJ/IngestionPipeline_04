select
    segmento,
    cnpj_base,
    nome_instituicao,
    tipo_registro,
    {{ normaliza_nome('nome_instituicao') }} as chave_nome
from {{ source('trusted', 'bancos') }}
