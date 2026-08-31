# Atv4 — Ingestão e Tratamento de Dados

Pipeline medalhão (**raw → trusted → delivery**) para o PECE-POLI Módulo 3.
Ingestão e limpeza técnica em **Python** (pydantic v2), transformação e regra de
negócio em **dbt**, base final em **Postgres**, com export final também em
**Parquet** em disco.

---

## Arquitetura

```
data/raw_free/            fontes originais (Bancos, Complains, Empregados)
        │
        ▼  Python — src/treatment.py + src/schemas.py (pydantic v2)
data/trusted_parquet/     trusted, schema explícito, quarentena em _rejected/
        │
        ▼  src/load.py → Postgres schema trusted_atv4
Postgres trusted_atv4
        │
        ▼  dbt — elt_dbt_atv4/models/
Postgres delivery_atv4    staging views + mart final
        │
        ▼  export_delivery.py
data/delivered_gold/      delivery também em Parquet, exigência do enunciado
```

## Como rodar

```bash
docker compose down -v                                   # opcional: reset total
docker compose up -d --build                              # build + sobe Postgres
docker compose run --rm app                                # raw → trusted → Postgres
docker compose run --rm elt_transformation dbt run         # trusted → delivery
docker compose run --rm elt_transformation dbt test        # valida invariantes
docker compose run --rm app uv run python export_delivery.py  # delivery → Parquet
```

---

## Camada Python — Raw → Trusted → Postgres

### Extração (raw)

**A etapa de extração foi deliberadamente pulada nesta execução**: os arquivos originais já estavam disponíveis
localmente em `data/raw_free/`, então o pipeline parte direto da validação
(`main.py` lê direto de `bronze_path/{Dataset}/...`).

### Schemas — validação com Pydantic v2

**Por que Pydantic: ** Pydantic valida **linha a linha** e produz
uma mensagem de erro específica por linha — exatamente o que alimenta a
quarentena (`_rejected/*.csv`) com um `_motivo` legível.

Quatro modelos — um por **schema real de origem**, não um por dataset. Isso
importa porque Empregados tem dois arquivos que **não compartilham schema**:

| Modelo | Tabela Trusted | Origem |
|---|---|---|
| `BankValidator` | `bancos` | 1 `.tsv` |
| `ComplaintsValidator` | `complaints` | 7 `.csv` (mesmo schema, unidos em Python) |
| `EmployerSegmentoValidator` | `employer_segments` | `..._v2.csv` (23ª coluna = `Segmento`) |
| `EmployerCnpjValidator` | `employer_cnpj` | `..._less_v2.csv` (23ª coluna = `CNPJ`) |

`EmployerSegmentoValidator` e `EmployerCnpjValidator` herdam de
`EmployerValidator` (as 22 colunas comuns) e cada um adiciona só a sua coluna
exclusiva — evita duplicar 22 campos em dois lugares.

**Padrões usados em todo modelo:**

- `extra="forbid"` — uma coluna nova ou renomeada na origem **estoura** em vez
  de sumir silenciosamente. É a rede de segurança contra mudança de schema não
  avisada.
- `populate_by_name=True` + `Field(alias=...)` — aceita o nome de coluna
  **exato** da origem (com espaço, acento, maiúscula: `"CNPJ IF"`, `"Índice"`)
  e mapeia para um atributo Python limpo (`cnpj_base`, `indice`).
- `@model_validator(mode="before")` — limpeza de string bruta *antes* da
  validação de tipo (`zfill` de CNPJ, `"1º"` → `1`, `" "` → `None`).
- `@model_validator(mode="after")` — regras que dependem de mais de um campo
  **já validado** (derivar `tipo_registro`, checar `soma == total`).

**Exemplo completo — `BankValidator`:**

```python
class BankValidator(BaseModel):
    """Trusted schema for `EnquadramentoInicia_v2.tsv` (Bancos)."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    segmento: Literal["S1", "S2", "S3", "S4", "S5"] = Field(alias="Segmento")
    cnpj_base: str = Field(alias="CNPJ", pattern=r"^\d{8}$")
    nome_instituicao: str = Field(alias="Nome", max_length=200)

    # Derivados: não vêm da origem
    tipo_registro: Literal["PRUDENCIAL", "INSTITUICAO"] | None = None
    nome_tem_caractere_invalido: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_raw(cls, data: dict) -> dict:
        cnpj = (data.get("CNPJ") or "").strip()
        if not cnpj:
            raise ValueError("CNPJ vazio")
        data["CNPJ"] = cnpj.zfill(8)
        data["Nome"] = (data.get("Nome") or "").strip()
        return data

    @model_validator(mode="after")
    def deriva_campos(self) -> "BankValidator":
        # Mojibake da origem: U+FFFD (cp1252 lido como UTF-8) e '?' literal.
        self.nome_tem_caractere_invalido = (
            "�" in self.nome_instituicao or "?" in self.nome_instituicao
        )
        # CNPJ raiz repetido entre conglomerado e instituição individual;
        # o sufixo "- PRUDENCIAL" é o único sinal que os distingue na origem.
        self.tipo_registro = (
            "PRUDENCIAL"
            if self.nome_instituicao.upper().endswith("- PRUDENCIAL")
            else "INSTITUICAO"
        )
        return self
```

O que esse modelo resolve, na prática:

- `Literal["S1", ..., "S5"]` já garante que só esses cinco valores passam —
  qualquer outro vira `ValidationError` sem código extra.
- `pattern=r"^\d{8}$"` garante exatamente 8 dígitos **depois** do `zfill` — sem
  isso, um CNPJ malformado (letras, tamanho errado) passaria disfarçado.
- `mode="after"` deriva `tipo_registro` a partir do sufixo do nome, resolvendo
  as 15 duplicatas de CNPJ **sem inventar uma chave nova** — a chave real vira
  `(cnpj_base, tipo_registro)`.
- `nome_tem_caractere_invalido` sinaliza a linha com mojibake em vez de tentar
  "consertar" acentos que não existem mais nos bytes originais.

Os outros três modelos seguem o mesmo formato — a diferença está no que cada
`before`/`after` resolve: `ComplaintsValidator` trata o marcador de nulo
`" "`, a vírgula decimal e a regra `procedentes+outras+não_reguladas=total`;
`EmployerValidator` trata o `"2000.0"` (round-trip de float do pandas) e a
categoria de faturamento grafada de duas formas.

### Sources — um leitor por dataset (`src/sources.py`)

Não existe um leitor genérico — cada dataset tem encoding, separador e
armadilhas diferentes, e um leitor compartilhado esconderia essas diferenças
até quebrar silenciosamente. Contrato comum: sempre devolve um DataFrame
**totalmente string** (`dtype=str, keep_default_na=False`), sem nenhuma
inferência de tipo do pandas — a tipagem é trabalho do Pydantic, não do leitor.

```python
def read_complaints(path: Path) -> pd.DataFrame:
    """Read 8 .csv complaints. Returns an all-strings DataFrame.

    Every line ends with a trailing ';', so pandas invents a phantom
    "Unnamed: 14" column. Drop it after confirming it's actually empty,
    not by position — a positional drop would silently eat a real column
    if a future source file's shape ever changes.
    """
    raw = pd.read_csv(path, sep=';', encoding="cp1252", dtype=str, keep_default_na=False)

    phantom = "Unnamed: 14"
    if phantom in raw.columns:
        assert raw[phantom].eq("").all(), f"{phantom} is not empty in {path.name} — trap assumption broke"
        raw = raw.drop(columns=phantom)

    return raw
```

Esse `assert` é o ponto mais importante do arquivo: **encontrado como bug
real durante a integração** — sem descartar essa coluna fantasma, 100% das
linhas dos 7 arquivos de Complains eram rejeitadas (`extra="forbid"` barrava a
coluna extra em toda linha). A correção só dropa a coluna depois de confirmar
programaticamente que está vazia, nunca por posição.

`read_banks` usa UTF-8 + tab; `read_employers` usa UTF-8 + `|`. Nenhum dos
dois precisa de tratamento especial de coluna fantasma — é uma armadilha
exclusiva de Complains, por causa do `;` final em toda linha.

### Treatment — validação, quarentena e escrita em Parquet (`src/treatment.py`)

Duas funções genéricas, compartilhadas por todos os datasets:

```python
def validate(raw: pd.DataFrame, model: type[BaseModel]) -> tuple[list[dict], list[dict]]:
    accepted: list[dict] = []
    rejected: list[dict] = []

    for offset, row in enumerate(raw.to_dict("records")):
        try:
            accepted.append(model.model_validate(row).model_dump())
        except ValidationError as error:
            motivo = "; ".join(
                f"{'.'.join(str(loc) for loc in erro['loc'])}: {erro['msg']}"
                for erro in error.errors()
            )
            rejected.append({"_linha": offset + 2, "_motivo": motivo, **row})

    return accepted, rejected
```

`_motivo` é construído a partir de `error.errors()` (campo + mensagem), não do
`str(error)` bruto do Pydantic — a versão bruta tem múltiplas linhas e uma URL
de documentação, inutilizável numa célula de CSV de quarentena.

`write_quarantine` grava `rejected` em `_rejected/{dataset}.csv` (no-op se
nada foi rejeitado); `consolidate_quarantine` funde fragmentos de quarentena
por arquivo (usado só por Complains, que tem 7 arquivos) num único CSV,
preservando de qual arquivo cada linha veio via uma coluna `_arquivo`.

Cada dataset tem uma função `treat_*` que amarra leitor → validação →
quarentena → **assert de reconciliação** → tipagem explícita → Parquet:

```python
def treat_banks(path: Path) -> pd.DataFrame:
    raw = read_banks(path)
    accepted, rejected = validate(raw, BankValidator)

    write_quarantine(rejected, "bancos")
    assert len(raw) == len(accepted) + len(rejected), "reconciliation broke for bancos"

    trusted = pd.DataFrame(accepted).astype({
        "segmento": "category",
        "cnpj_base": "string",
        "nome_instituicao": "string",
        "tipo_registro": "category",
        "nome_tem_caractere_invalido": "bool",
    })

    target = Path(path_strings.trusted_path) / "bancos.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    trusted.to_parquet(target, index=False)
    return trusted
```

O `assert` é o invariante `linhas_raw == linhas_trusted + linhas_rejeitadas` —
se ele falhar, uma linha desapareceu silenciosamente em algum lugar, o que não
pode acontecer no design (quarentena, nunca descarte).

`category` é usado para colunas de conjunto fechado e baixa cardinalidade
repetidas muitas vezes (`segmento`: 5 valores em 1.474 linhas) — dicionariza o
valor em vez de repetir a string, menor em disco e mais rápido para
agrupar/filtrar depois. `Int64` (maiúsculo) é o tipo inteiro **anulável** do
pandas — necessário nas três colunas de contagem de clientes de Complains e em
`employer_founded`, porque reconstruir um DataFrame a partir de dicts
validados promove silenciosamente `int` com `None` para `float64`; `Int64`
evita isso.

`treat_complaints`, `treat_employer_segment` e `treat_employer_cnpj` seguem a
mesma forma, cada um com seu próprio mapa de dtypes e sua própria armadilha:
`treat_complaints` também captura `EmptyDataError` explicitamente (o arquivo
`2022_tri_02_nao_ha_dados.csv`, 0 bytes) e segue sem quebrar a execução.

### Load — Postgres (`src/load.py`)

```python
def push_to_db(df, table_name, engine, schema="trusted_atv4", if_exists="append") -> None:
    if if_exists == "replace" and inspect(engine).has_table(table_name, schema=schema):
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}"'))
        if_exists = "append"

    df.to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists,
               index=False, chunksize=10000)
```

`"replace"` **trunca** a tabela em vez de fazer `DROP`/recriar. Motivo: o dbt
constrói views diretamente em cima dessas tabelas (`stg_bancos` depende de
`trusted_atv4.bancos`) — um `DROP TABLE` falha assim que essa view existe
(`DependentObjectsStillExist`), e um `DROP ... CASCADE` destruiria a camada
dbt inteira a cada reingestão. Truncar mantém o objeto da tabela (e tudo que
depende dela) intacto; só as linhas mudam.

`load_dataset(prefix, table_name, engine)` junta fragmentos Parquet que
começam com `prefix` (os 7 de Complains, por exemplo) num único DataFrame via
`pd.concat` antes de chamar `push_to_db` — assim os 7 arquivos viram **uma**
tabela lógica no Postgres. `export_table_to_parquet` faz o caminho inverso:
lê uma tabela do Postgres com `run_query` (`SELECT *`) e escreve em
`data/delivered_gold/{table}.parquet` — usado pelo `export_delivery.py` para
satisfazer a exigência do enunciado de delivery também em disco.

### `main.py` — como tudo se conecta

```python
def main():
    treat_banks(bancos_path)

    for file in complaints_path.iterdir():
        if file.is_file():
            treat_complaints(file)
    consolidate_quarantine("complaints_", "complaints")

    treat_employer_cnpj(employers_cnpj_path)
    treat_employer_segment(employers_segments_path)

    engine = ld.get_engine()
    ld.load_dataset("bancos", "bancos", engine)
    ld.load_dataset("complaints_", "complaints", engine)
    ld.load_dataset("employer_segments", "employer_segments", engine)
    ld.load_dataset("employer_cnpj", "employer_cnpj", engine)
```

Ordem importa: primeiro todos os `treat_*` (raw → trusted, grava Parquet em
disco), só depois o `load_dataset` por tabela (trusted Parquet → Postgres).
Complains é o único caso com um loop — as outras três fontes são um arquivo
por tabela, então uma chamada basta.

---

## Relatório de qualidade dos dados

### Bancos — `EnquadramentoInicia_v2.tsv` (1.474 linhas)

- **Mojibake irrecuperável em `nome_instituicao`.** 3.109 ocorrências de `U+FFFD`
  (`�`) e 6 `?` literais. O arquivo já chegou corrompido — alguém provavelmente decodificou
  cp1252 como UTF-8 com `errors="replace"` antes deste pipeline recebê-lo, e os
  bytes originais não existem mais. `encoding="latin1"` **não** recupera isso.
  Decisão: não inventar acentos. O defeito é documentado e sinalizado por linha
  via a flag `nome_tem_caractere_invalido`, nunca corrigido silenciosamente.
- **`cnpj_base` não é chave primária** — 15 duplicatas, e são significativas: o
  arquivo mistura o *conglomerado prudencial* e a *instituição individual* sob o
  mesmo CNPJ raiz (ex.: `BRADESCO - PRUDENCIAL` vs `BRADESCO S.A.`). Resolvido
  derivando `tipo_registro` (`PRUDENCIAL`/`INSTITUICAO`) a partir do sufixo do
  nome — chave real passa a ser `(cnpj_base, tipo_registro)`.

### Complains — 7 arquivos trimestrais + 1 vazio (918 linhas)

- **Trimestre vazio.** `2022_tri_02_nao_ha_dados.csv` tem **0 bytes** — não é
  "cabeçalho sem linhas", é vazio. `pd.read_csv` lança
  `EmptyDataError`; o pipeline captura isso explicitamente, loga e segue para os
  outros arquivos sem quebrar a execução.
- **Toda linha termina com `;`**, o que faz o pandas inventar uma 15ª coluna
  fantasma (`Unnamed: 14`). Sem tratar isso, **100% das linhas dos 7 arquivos
  eram rejeitadas** (a coluna extra violava `extra="forbid"` do schema) — bug
  real encontrado durante a integração, corrigido no leitor: a coluna só é
  descartada depois de confirmar programaticamente que está 100% vazia, nunca
  por posição.
- **Nulo é um espaço simples `" "`,** não string vazia. Afeta 14 colunas;
  `Índice` (651/918) e `CNPJ IF` (481/918) são as piores. `df.isna().sum()`
  reportaria zero nulos sem esse tratamento explícito.
- **`CNPJ IF` ausente não é defeito.** Correlaciona perfeitamente com `Tipo`:
  Conglomerado 481/481 sem CNPJ, Banco/financeira 437/437 com 8 dígitos. Virou
  regra de negócio testada (`cnpj_base is not None ⟺ tipo == 'Banco/financeira'`),
  não uma limpeza.
- **`Categoria` muda de taxonomia entre 2021 e 2022** (BACEN renomeou as
  faixas). Preservado como veio — harmonizar é decisão de negócio, não de
  Trusted, e ficaria em dbt/Delivery caso seja necessário.
- **Invariante verificada:** `procedentes + outras + não_reguladas = total` vale
  para as 918/918 linhas — validado tanto em Python (regra de rejeição) quanto
  como teste dbt singular.
- **Resultado final: 918/918 linhas aceitas, zero rejeitadas.**

### Empregados — 2 arquivos com schemas diferentes (34 + 5 linhas)

- **Os dois arquivos não compartilham schema.** 22 colunas idênticas; a 23ª
  difere: `..._v2.csv` (34 linhas) traz `Segmento`, `..._less_v2.csv` (5 linhas)
  traz `CNPJ`. Tratados como **duas tabelas Trusted separadas**
  (`employer_segments`, `employer_cnpj`) em vez de concatenados — concatenar
  ingenuamente geraria 39 linhas com 5 duplicatas.
- **`_less_v2` é um subconjunto, não dado adicional.** Os 5 `employer_name` do
  arquivo `_less_` já existem entre os 34 do `_v2` — mesmas empresas, apenas com
  chave de join diferente (CNPJ em vez de Segmento). Não agrega identidade
  nova, por isso o mart final é construído só a partir de `employer_segments`.
  *(Pergunta original em aberto para o professor — resposta recebida: o
  dataset foi preparado deliberadamente assim, como parte do exercício.)*
- **`employer-founded` chegava como `"2000.0"`** — impressão digital de um
  round-trip do pandas (coluna com nulos virou `float64`, `to_csv` escreveu
  `.0`). Corrigido para `Int16` nullable de verdade.
- **`employer-revenue` tinha a mesma categoria grafada de duas formas:**
  `Desconhecida/não se aplica` vs `Desconhecido/Não se aplica`. Canonicalizado
  para uma só forma.
- **Separador decimal é PONTO aqui** (`3.8`, `77.0`) — oposto de Complains, que
  usa vírgula. Os dois leitores são deliberadamente independentes por causa
  disso; um leitor genérico compartilhado quebraria um dos dois.

---

## Camada dbt — Trusted → Delivery

### Fronteira Python × dbt (regra do projeto)

- **Unir arquivos da MESMA tabela → Python.** (os 7 CSVs trimestrais de Complains)
- **Unir tabelas DIFERENTES → dbt.** (Bancos × Complains × Empregados)
- **Tipagem → Python.** É o que dá schema ao Parquet; o Postgres já recebe tipado.
- **Regra de negócio / decisão analítica → dbt.**

### Estrutura

```
elt_dbt_atv4/
  dbt_project.yml
  macros/
    normaliza_nome.sql
    generate_schema_name.sql
  models/
    schema.yml                              # testes
    staging/
      sources.yml
      stg_bancos.sql
      stg_complains.sql
      stg_empregados.sql
    marts/
      delivery_reclamacoes_satisfacao.sql
  tests/
    assert_complains_totals_match.sql       # teste singular
```

### Macro `normaliza_nome` — a regra de join em um lugar só

```sql
{% macro normaliza_nome(coluna) %}
trim(regexp_replace(
  regexp_replace(
    regexp_replace(
      regexp_replace(upper({{ coluna }}), '\s*[-–]\s*PRUDENCIAL$', '', 'g'),
      '\s*\(CONGLOMERADO\)$', '', 'g'),
    '[^A-Z0-9 ]', '', 'g'),
  '\s+', ' ', 'g'))
{% endmacro %}
```

`UPPER` → remove sufixo `- PRUDENCIAL` → remove sufixo `(CONGLOMERADO)` →
remove pontuação → colapsa espaços → `trim`. Centralizada porque é usada nas
três staging models (`stg_bancos`, `stg_complains`, `stg_empregados`) — se a
regra mudasse em um lugar só, chaves normalizadas de formas diferentes param
de casar **silenciosamente** (o join devolve zero linhas, sem erro nenhum).
Uma macro única evita esse tipo de divergência por construção.

### Macro `generate_schema_name` — um gotcha clássico do dbt

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

O comportamento **padrão** do dbt não usa um `schema` customizado como veio —
ele **concatena** com o schema default (`{target.schema}_{custom_schema}`),
justamente para evitar colisões entre projetos. Sem essa macro, o mart com
`config(schema='delivery_atv4')` teria virado literalmente
`delivery_atv4_delivery_atv4` no Postgres. Esse override (o snippet padrão da
própria documentação do dbt) faz o schema customizado ser usado ao pé da
letra.

### `sources.yml` — o contrato com o que o Python escreveu

```yaml
version: 2
sources:
  - name: trusted
    schema: trusted_atv4
    tables:
      - name: bancos
      - name: complaints
      - name: employer_segments
      - name: employer_cnpj
```

Os nomes de tabela aqui têm que bater **exatamente** com o que
`load_dataset(..., table_name, ...)` escreveu em `main.py` — uma divergência
vira `relation does not exist` só na hora do `dbt run`, nunca antes.

### `stg_bancos.sql`

```sql
select
    segmento,
    cnpj_base,
    nome_instituicao,
    tipo_registro,
    {{ normaliza_nome('nome_instituicao') }} as chave_nome
from {{ source('trusted', 'bancos') }}
```

Só seleciona `tipo_registro` — não regenera a coluna com uma expressão SQL
própria. Ela já vem calculada do `BankValidator.deriva_campos()` em Python;
recalculá-la aqui duplicaria a mesma regra de negócio em dois lugares que
poderiam divergir silenciosamente se um dos dois mudasse sozinho.

### `stg_complains.sql`

```sql
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
```

`trimestre` só recebe um `cast`, sem `replace(trimestre, 'º', '')` — o
Python já limpou o `"1º"` → `1` antes de gravar o Trusted; repetir a limpeza
aqui seria redundante (e o dado real já chega como texto `'1'`..`'4'`, sem
`º` nenhum para remover).

### `stg_empregados.sql`

```sql
with base as (
    select
        employer_name, nome_instituicao, segmento, nota_geral,
        nota_cultura_valores, nota_qualidade_vida, nota_remuneracao_beneficios,
        pct_recomendam, match_percent,
        {{ normaliza_nome('nome_instituicao') }} as chave_nome,
        row_number() over (
            partition by {{ normaliza_nome('nome_instituicao') }}
            order by match_percent desc, employer_name
        ) as rn
    from {{ source('trusted', 'employer_segments') }}
)
select employer_name, nome_instituicao, segmento, nota_geral,
       nota_cultura_valores, nota_qualidade_vida, nota_remuneracao_beneficios,
       pct_recomendam, match_percent, chave_nome
from base
where rn = 1
```

Único modelo com lógica de deduplicação de verdade: `nome_instituicao` tem
2 duplicatas em `employer_segments` (risco de fan-out no join final).
`row_number()` numera dentro de cada `chave_nome`, priorizando maior
`match_percent`; `employer_name` no `order by` é desempate determinístico —
sem ele, empatados alternariam entre execuções. A fonte é só
`employer_segments`: como `employer_cnpj` é um subconjunto puro por
`employer_name` (nenhuma empresa nova), incluí-lo não mudaria o resultado.

*(Nota de implementação: `select * exclude (rn)` — sintaxe do Snowflake/DuckDB
— não existe em Postgres nativo, mesmo em versões recentes; listar as colunas
explicitamente foi a correção aplicada.)*

### `delivery_reclamacoes_satisfacao.sql` — o mart final

```sql
{{ config(materialized='table', schema='delivery_atv4') }}

select
    c.ano, c.trimestre, c.categoria, c.tipo, c.instituicao_financeira,
    b.segmento, b.cnpj_base,
    c.indice, c.qtd_recl_total, c.qtd_recl_reguladas_procedentes, c.qtd_clientes_ccs_scr,
    e.employer_name, e.nota_geral, e.nota_qualidade_vida,
    e.nota_remuneracao_beneficios, e.pct_recomendam, e.match_percent
from {{ ref('stg_complains') }} c
left join {{ ref('stg_empregados') }} e on c.chave_nome = e.chave_nome
left join {{ ref('stg_bancos') }}     b on c.chave_nome = b.chave_nome
                                       and b.tipo_registro = 'INSTITUICAO'
```

`left join` de propósito, a partir de Complains: preserva as 918 linhas de
reclamações, com métricas do Glassdoor nulas onde não houve match — um
`inner join` descartaria ~87% do fato. `b.tipo_registro = 'INSTITUICAO'` no
join com Bancos evita casar com a linha "- PRUDENCIAL" do mesmo CNPJ raiz.

### Testes declarados (`models/schema.yml` + teste singular)

```yaml
models:
  - name: stg_bancos
    columns:
      - name: segmento
        tests:
          - accepted_values: {values: ['S1','S2','S3','S4','S5']}
  - name: stg_complains
    columns:
      - name: ano
        tests: [not_null]
      - name: trimestre
        tests: [not_null]
      - name: qtd_recl_total
        tests: [not_null]
      - name: tipo
        tests:
          - accepted_values: {values: ['Conglomerado', 'Banco/financeira']}
  - name: stg_empregados
    columns:
      - name: chave_nome
        tests: [unique, not_null]
```

```sql
-- tests/assert_complains_totals_match.sql
-- Falha (retorna linhas) se procedentes + outras + nao_reguladas != total.
select *
from {{ ref('stg_complains') }}
where qtd_recl_reguladas_procedentes
    + qtd_recl_reguladas_outras
    + qtd_recl_nao_reguladas
    != qtd_recl_total
```

Um teste singular em dbt é uma query que **deveria devolver zero linhas** —
qualquer linha retornada é uma violação da regra. **8/8 testes passando** na
execução final.

---

## Join final e taxa de match

**A descoberta que definiu o join:** a chave tem que ser o nome normalizado, não
o CNPJ. Testado nos dados reais:

| Estratégia de join | Linhas com match |
|---|---|
| Por `cnpj` | 16 |
| Por nome normalizado | **119** (21 dos 32 empregadores) |

Motivo: as empresas do Glassdoor aparecem em Complains como
`tipo = 'Conglomerado'`, e essas linhas **não têm CNPJ** — um join por CNPJ
descarta a maior parte dos matches possíveis por construção.

| Métrica | Valor |
|---|---|
| Total de linhas | **918** |
| Linhas com `nota_geral` preenchida (match) | **119** |
| Taxa de match | **~13%** |

---

## Validação executada

- **Reconciliação:** `linhas_raw == linhas_trusted + linhas_rejeitadas` vale em
  todo o pipeline; zero linhas rejeitadas na execução final.
- **8/8 testes dbt passando:** `not_null`, `unique`, `accepted_values` e o
  teste singular de soma.
- **Delivery exportado em dois formatos**, conforme exigido: tabela Postgres
  (`delivery_atv4.delivery_reclamacoes_satisfacao`) e Parquet em disco
  (`data/delivered_gold/`).

---

## Decisões e limitações conhecidas

- Trusted é limpeza técnica; nenhuma regra de negócio ou harmonização de
  taxonomia foi aplicada nessa camada — tudo isso vive em dbt.
- O mojibake de Bancos é sinalizado, não corrigido — os bytes originais não
  existem mais em lugar nenhum.
- `employer_cnpj` (5 linhas) é carregado no Trusted e registrado como source no
  dbt, mas não participa do mart final — não agrega identidade além do que
  `employer_segments` já cobre.
- `if_exists="replace"` em `push_to_db` trunca em vez de recriar a tabela, para
  não quebrar as views do dbt que dependem dela em reingestões subsequentes.
