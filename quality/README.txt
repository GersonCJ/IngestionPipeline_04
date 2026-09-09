# Data Quality com Great Expectations

## Objetivo

Esta camada implementa validações de qualidade de dados para o pipeline desenvolvido na Atividade 4, utilizando Great Expectations.

As validações cobrem as camadas Trusted e Delivery/Gold e foram estruturadas para funcionar de forma independente da ferramenta de orquestração.

A solução é independente do orquestrador — cada validação devolve exit code — e está integrada ao Apache Airflow como gates da DAG do pipeline.

---

## Arquitetura

```text
RAW
 │
 ▼
Python + Pydantic
 │
 ▼
Trusted Parquet
 │
 ▼
Great Expectations
Quality Gate - Trusted
 │
 ▼
PostgreSQL
 │
 ▼
dbt
 │
 ▼
Delivery / Gold
 │
 ▼
Great Expectations
Quality Gate - Delivery
```

---

## Datasets validados

### 1. Trusted - Bancos

Arquivo:

```text
data/trusted_parquet/bancos.parquet
```

Principais validações:

- segmento dentro do domínio S1 a S5;
- CNPJ base obrigatório;
- CNPJ base com exatamente 8 dígitos;
- nome da instituição obrigatório;
- tipo de registro dentro do domínio esperado.

Total:

```text
5 Expectations
```

---

### 2. Trusted - Complaints

Arquivos:

```text
data/trusted_parquet/complaints_*.parquet
```

Os arquivos trimestrais são consolidados em um único dataset lógico antes da validação.

Principais validações:

- quantidade esperada de registros;
- ano obrigatório;
- trimestre entre 1 e 4;
- tipo dentro do domínio esperado;
- valores de reclamações não negativos;
- validação da soma dos tipos de reclamação contra o total informado.

Total:

```text
9 Expectations
```

---

### 3. Trusted - Employer Segments

Arquivo:

```text
data/trusted_parquet/employer_segments.parquet
```

Principais validações:

- quantidade esperada de registros;
- employer obrigatório;
- instituição obrigatória;
- segmento dentro do domínio esperado;
- contadores não negativos;
- notas entre 0 e 5;
- percentuais entre 0 e 100.

Total:

```text
18 Expectations
```

---

### 4. Trusted - Employer CNPJ

Arquivo:

```text
data/trusted_parquet/employer_cnpj.parquet
```

Principais validações:

- quantidade esperada de registros;
- employer obrigatório;
- unicidade de employer;
- instituição obrigatória;
- CNPJ base obrigatório;
- CNPJ base único;
- CNPJ base com exatamente 8 dígitos;
- contadores não negativos;
- notas entre 0 e 5;
- percentuais entre 0 e 100.

Total:

```text
21 Expectations
```

---

### 5. Delivery / Gold

Arquivo:

```text
data/delivered_gold/delivery_reclamacoes_satisfacao.parquet
```

Principais validações:

- quantidade esperada de registros;
- conjunto esperado de colunas;
- campos obrigatórios;
- ano dentro do período esperado;
- trimestre entre 1 e 4;
- tipo dentro do domínio esperado;
- quantidade de reclamações não negativa;
- notas entre 0 e 5 quando há correspondência com dados de empregados;
- percentuais entre 0 e 100;
- proporção esperada de registros com match.

Total:

```text
20 Expectations
```

---

## Reconciliação entre camadas

Além das Expectations do Great Expectations, é realizada uma reconciliação entre a camada Trusted de reclamações e a Delivery.

Resultado esperado:

```text
Complaints Trusted: 918
Delivery Gold:      918
```

Essa verificação garante que as transformações e joins executados pelo dbt não provoquem perda ou multiplicação indevida de registros.

---

## Cobertura de qualidade

Resumo das validações implementadas:

```text
Bancos                  5
Complaints              9
Employer Segments      18
Employer CNPJ          21
Delivery / Gold        20
--------------------------
Total                   73
```

Resultado final validado:

```text
73 / 73 Expectations aprovadas
```

Também foi validada com sucesso a reconciliação entre Complaints Trusted e Delivery.

---

## Estrutura da pasta

```text
quality/
│
├── gx_context.py
│
├── explore_bancos.py
├── explore_complaints.py
├── explore_employer_segments.py
├── explore_employer_cnpj.py
├── explore_delivery.py
│
├── validate_trusted_bancos.py
├── validate_trusted_complaints.py
├── validate_trusted_employer_segments.py
├── validate_trusted_employer_cnpj.py
├── validate_delivery.py
│
├── run_quality.py
└── README.txt
```

Os scripts `explore_*.py` foram utilizados para profiling e entendimento dos datasets antes da criação das regras de qualidade.

Os scripts `validate_*.py` executam as validações de cada domínio. Cada um expõe uma função `run() -> bool` e também funciona como script isolado, retornando exit code.

O arquivo `gx_context.py` centraliza o Data Context compartilhado: abertura do contexto persistente em `/app/gx`, criação idempotente de data source, asset e batch definition, execução da validation definition e geração dos Data Docs.

O arquivo `run_quality.py` atua como executor central de todas as validações.

---

## Data Context persistente

O contexto do Great Expectations é file-based, gravado no volume `gx_docs`, montado em `/app/gx`.

Com isso, cada execução acumula histórico e os Data Docs ficam navegáveis em:

```text
http://localhost:8182
```

Contexto persistente exige idempotência: `add_pandas`, `add_dataframe_asset` e `add_batch_definition_whole_dataframe` levantam erro quando o objeto já existe, o que só aparece a partir da segunda execução.

Por isso todo acesso em `gx_context.py` busca o objeto primeiro e só cria no `LookupError`.

---

## Execução

### Pela DAG do Airflow

As validações são duas tasks da DAG `atv4_medallion_end_to_end`:

```text
gx_trusted     — entre a escrita dos Parquet e a carga no Postgres
gx_delivery    — depois do export da camada Gold
```

Ambas rodam na imagem `ingestion_atv4`, via `src/cli.py`.

### Manualmente

Todas as camadas de uma vez:

```bash
docker compose --profile build run --rm app uv run python quality/run_quality.py
```

Ou uma camada por vez, do mesmo jeito que o Airflow chama:

```bash
docker compose --profile build run --rm app uv run python -m src.cli quality-trusted
docker compose --profile build run --rm app uv run python -m src.cli quality-delivery
```

---

## Resultado esperado

Quando todas as validações passam:

```text
============================================================
 RESUMO DAS VALIDAÇÕES
============================================================

Trusted - Bancos                    PASSOU
Trusted - Complaints                PASSOU
Trusted - Employer Segments         PASSOU
Trusted - Employer CNPJ             PASSOU
Delivery / Gold                     PASSOU

============================================================
 QUALIDADE GERAL: PASSOU
============================================================
```

O processo retorna:

```text
exit code 0
```

---

## Tratamento de falhas

Cada script de validação retorna:

```text
0 = sucesso
1 = falha
```

O executor central utiliza esses códigos para determinar o resultado geral da camada de qualidade.

Se qualquer validação falhar:

```text
QUALIDADE GERAL: FALHOU
```

e o processo retorna:

```text
exit code 1
```

---

## Teste controlado de falha

Foi realizado um teste negativo alterando temporariamente a regra de segmentos da validação de Bancos.

A regra válida:

```text
S1, S2, S3, S4, S5
```

foi temporariamente reduzida para:

```text
S1
```

O Great Expectations identificou corretamente a inconsistência.

O resultado consolidado passou a apresentar:

```text
Trusted - Bancos                    FALHOU
QUALIDADE GERAL:                    FALHOU
```

e o processo retornou:

```text
exit code 1
```

Após o teste, a regra original foi restaurada e todas as validações voltaram a apresentar sucesso.

---

## Orquestração

A camada de qualidade foi desenvolvida de forma independente da ferramenta de orquestração: cada validação expõe `run() -> bool` e cada script devolve exit code.

Essa integração está implementada. No Apache Airflow, a DAG `atv4_medallion_end_to_end` executa as validações como duas tasks próprias, através de `src/cli.py`:

```text
raw_to_trusted → gx_trusted → load_postgres → dbt_run → dbt_test
    → dbt_docs_generate → export_delivery → gx_delivery → openmetadata_catalog
```

Os gates são duros. Quando `gx_trusted` reprova, a etapa levanta exceção, a task falha e todas as tasks seguintes ficam em `upstream_failed`. Na prática, isso significa que dado reprovado não chega ao Postgres nem à camada Delivery.

O mesmo comportamento vale para `gx_delivery`, com a diferença de que ali os dados já foram materializados: a falha sinaliza o problema e impede que o catálogo seja publicado sobre uma Gold inválida.
