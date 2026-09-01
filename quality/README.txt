# Data Quality com Great Expectations

## Objetivo

Esta camada implementa validações de qualidade de dados para o pipeline desenvolvido na Atividade 4, utilizando Great Expectations.

As validações cobrem as camadas Trusted e Delivery/Gold e foram estruturadas para funcionar de forma independente da ferramenta de orquestração.

A solução foi preparada para futura integração com Apache Airflow por meio de exit codes do processo.

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
├── Dockerfile
├── requirements.txt
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
└── README.md
```

Os scripts `explore_*.py` foram utilizados para profiling e entendimento dos datasets antes da criação das regras de qualidade.

Os scripts `validate_*.py` executam as validações de cada domínio.

O arquivo `run_quality.py` atua como executor central de todas as validações.

---

## Execução

### Construir a imagem Docker

Na raiz do projeto:

```bash
docker build -f quality/Dockerfile -t ingestion-gx .
```

---

### Executar todas as validações

No PowerShell:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace ingestion-gx python quality/run_quality.py
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

## Preparação para orquestração

A camada de qualidade foi desenvolvida de forma independente da ferramenta de orquestração.

O executor:

```text
quality/run_quality.py
```

já está preparado para ser chamado futuramente pelo Apache Airflow ou outro orquestrador.

O orquestrador poderá utilizar o exit code do processo para decidir se o pipeline deve prosseguir ou ser interrompido.

Nesta implementação, a integração com Airflow ainda não foi realizada.
