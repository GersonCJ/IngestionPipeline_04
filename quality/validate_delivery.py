from pathlib import Path
import sys

import pandas as pd
import great_expectations as gx

from great_expectations.expectations.row_conditions import Column


# =========================================================
# 1. CARREGAR DELIVERY
# =========================================================

delivery_path = Path(
    "data/delivered_gold/delivery_reclamacoes_satisfacao.parquet"
)

delivery = pd.read_parquet(delivery_path)

print(
    f"Delivery carregada: "
    f"{len(delivery)} linhas / {len(delivery.columns)} colunas"
)


# =========================================================
# 2. COMPLAINTS PARA RECONCILIAÇÃO
# =========================================================

trusted_path = Path("data/trusted_parquet")

complaint_files = sorted(
    trusted_path.glob("complaints_*.parquet")
)

complaints = pd.concat(
    [pd.read_parquet(file) for file in complaint_files],
    ignore_index=True,
)


reconciliation_success = (
    len(delivery) == len(complaints)
)


# =========================================================
# 3. GREAT EXPECTATIONS
# =========================================================

context = gx.get_context(
    mode="ephemeral"
)

data_source = context.data_sources.add_pandas(
    name="delivery_runtime"
)

asset = data_source.add_dataframe_asset(
    name="delivery_dataframe"
)

batch_definition = asset.add_batch_definition_whole_dataframe(
    name="delivery_batch"
)


suite = gx.ExpectationSuite(
    name="delivery_quality"
)


# =========================================================
# 4. ESTRUTURA
# =========================================================

suite.add_expectation(
    gx.expectations.ExpectTableRowCountToEqual(
        value=918
    )
)


expected_columns = [
    "ano",
    "trimestre",
    "categoria",
    "tipo",
    "instituicao_financeira",
    "segmento",
    "cnpj_base",
    "indice",
    "qtd_recl_total",
    "qtd_recl_reguladas_procedentes",
    "qtd_clientes_ccs_scr",
    "employer_name",
    "nota_geral",
    "nota_qualidade_vida",
    "nota_remuneracao_beneficios",
    "pct_recomendam",
    "match_percent",
]


suite.add_expectation(
    gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=expected_columns
    )
)


# =========================================================
# 5. COMPLETUDE
# =========================================================

for column in [
    "ano",
    "trimestre",
    "categoria",
    "tipo",
    "instituicao_financeira",
    "qtd_recl_total",
    "qtd_recl_reguladas_procedentes",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column=column
        )
    )


# =========================================================
# 6. DOMÍNIO
# =========================================================

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="ano",
        min_value=2021,
        max_value=2022,
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="trimestre",
        value_set=[1, 2, 3, 4],
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="tipo",
        value_set=[
            "Conglomerado",
            "Banco/financeira",
        ],
    )
)


for column in [
    "qtd_recl_total",
    "qtd_recl_reguladas_procedentes",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
        )
    )


# =========================================================
# 7. DADOS DE EMPREGADOS
# =========================================================

matched_rows = Column(
    "employer_name"
).is_not_null()


for column in [
    "nota_geral",
    "nota_qualidade_vida",
    "nota_remuneracao_beneficios",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
            max_value=5,
            row_condition=matched_rows,
        )
    )


for column in [
    "pct_recomendam",
    "match_percent",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
            max_value=100,
            row_condition=matched_rows,
        )
    )


# =========================================================
# 8. PROPORÇÃO DE MATCH
# =========================================================

suite.add_expectation(
    gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
        column="employer_name",
        min_value=0.129,
        max_value=0.130,
    )
)


# =========================================================
# 9. EXECUÇÃO
# =========================================================

suite = context.suites.add(
    suite
)


validation_definition = gx.ValidationDefinition(
    name="validate_delivery",
    data=batch_definition,
    suite=suite,
)


validation_definition = (
    context.validation_definitions.add(
        validation_definition
    )
)


result = validation_definition.run(
    batch_parameters={
        "dataframe": delivery
    }
)


# =========================================================
# 10. RESULTADOS
# =========================================================

print("\n=== RECONCILIAÇÃO ENTRE CAMADAS ===")

print(
    f"Complaints Trusted: {len(complaints)}"
)

print(
    f"Delivery Gold:      {len(delivery)}"
)

print(
    f"Reconciliação: "
    f"{'PASSOU' if reconciliation_success else 'FALHOU'}"
)


print("\n========================================")
print(" GREAT EXPECTATIONS — DELIVERY / GOLD")
print("========================================")

print(
    f"Resultado GX: "
    f"{'PASSOU' if result.success else 'FALHOU'}"
)

print(
    f"Expectations avaliadas: "
    f"{result.statistics['evaluated_expectations']}"
)

print(
    f"Expectations aprovadas: "
    f"{result.statistics['successful_expectations']}"
)

print(
    f"Expectations reprovadas: "
    f"{result.statistics['unsuccessful_expectations']}"
)

print(
    f"Taxa de sucesso: "
    f"{result.statistics['success_percent']:.2f}%"
)


# =========================================================
# 11. EXIT CODE
# =========================================================

if not result.success or not reconciliation_success:
    sys.exit(1)

sys.exit(0)