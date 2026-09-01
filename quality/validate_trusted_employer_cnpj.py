import sys

import pandas as pd
import great_expectations as gx


# =========================================================
# 1. CARREGAR DATASET
# =========================================================

path = "data/trusted_parquet/employer_cnpj.parquet"

df = pd.read_parquet(path)

print(
    f"Employer CNPJ carregado: "
    f"{len(df)} linhas / {len(df.columns)} colunas"
)


# =========================================================
# 2. GREAT EXPECTATIONS
# =========================================================

context = gx.get_context(
    mode="ephemeral"
)

data_source = context.data_sources.add_pandas(
    name="employer_cnpj_runtime"
)

asset = data_source.add_dataframe_asset(
    name="employer_cnpj_dataframe"
)

batch_definition = asset.add_batch_definition_whole_dataframe(
    name="employer_cnpj_batch"
)


# =========================================================
# 3. EXPECTATION SUITE
# =========================================================

suite = gx.ExpectationSuite(
    name="trusted_employer_cnpj_quality"
)


# ---------------------------------------------------------
# Estrutura
# ---------------------------------------------------------

suite.add_expectation(
    gx.expectations.ExpectTableRowCountToEqual(
        value=5
    )
)


# ---------------------------------------------------------
# Employer
# ---------------------------------------------------------

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="employer_name"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(
        column="employer_name"
    )
)


# ---------------------------------------------------------
# Instituição
# ---------------------------------------------------------

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="nome_instituicao"
    )
)


# ---------------------------------------------------------
# CNPJ BASE
# ---------------------------------------------------------

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="cnpj_base"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(
        column="cnpj_base"
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="cnpj_base",
        regex=r"^\d{8}$",
    )
)


# ---------------------------------------------------------
# Contadores
# ---------------------------------------------------------

for column in [
    "reviews_count",
    "culture_count",
    "salaries_count",
    "benefits_count",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
        )
    )


# ---------------------------------------------------------
# Notas
# ---------------------------------------------------------

for column in [
    "nota_geral",
    "nota_cultura_valores",
    "nota_diversidade_inclusao",
    "nota_qualidade_vida",
    "nota_alta_lideranca",
    "nota_remuneracao_beneficios",
    "nota_oportunidades_carreira",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
            max_value=5,
        )
    )


# ---------------------------------------------------------
# Percentuais
# ---------------------------------------------------------

for column in [
    "pct_recomendam",
    "pct_perspectiva_positiva",
    "match_percent",
]:
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=0,
            max_value=100,
        )
    )


# =========================================================
# 4. REGISTRAR SUITE
# =========================================================

suite = context.suites.add(
    suite
)


# =========================================================
# 5. VALIDATION DEFINITION
# =========================================================

validation_definition = gx.ValidationDefinition(
    name="validate_trusted_employer_cnpj",
    data=batch_definition,
    suite=suite,
)

validation_definition = (
    context.validation_definitions.add(
        validation_definition
    )
)


# =========================================================
# 6. EXECUTAR
# =========================================================

result = validation_definition.run(
    batch_parameters={
        "dataframe": df
    }
)


# =========================================================
# 7. RESULTADO
# =========================================================

print("\n==========================================")
print(" GREAT EXPECTATIONS — EMPLOYER CNPJ")
print("==========================================")

print(
    f"Resultado geral: "
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
# 8. EXIT CODE
# =========================================================

if not result.success:
    sys.exit(1)

sys.exit(0)