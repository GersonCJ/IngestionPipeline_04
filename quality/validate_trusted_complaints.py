from pathlib import Path
import sys

import pandas as pd
import great_expectations as gx


# =========================================================
# 1. CARREGAR COMPLAINTS
# =========================================================

trusted_path = Path("data/trusted_parquet")

files = sorted(
    trusted_path.glob("complaints_*.parquet")
)

if not files:
    print("ERRO: nenhum arquivo complaints_*.parquet encontrado.")
    sys.exit(1)


complaints = pd.concat(
    [pd.read_parquet(file) for file in files],
    ignore_index=True,
)


print(
    f"Complaints carregado: "
    f"{len(files)} arquivos / {len(complaints)} linhas"
)


# =========================================================
# 2. COLUNA TÉCNICA PARA RECONCILIAÇÃO
# =========================================================

complaints["_soma_reclamacoes"] = (
    complaints["qtd_recl_reguladas_procedentes"]
    + complaints["qtd_recl_reguladas_outras"]
    + complaints["qtd_recl_nao_reguladas"]
)


# =========================================================
# 3. GREAT EXPECTATIONS
# =========================================================

context = gx.get_context(
    mode="ephemeral"
)

data_source = context.data_sources.add_pandas(
    name="complaints_runtime"
)

asset = data_source.add_dataframe_asset(
    name="complaints_dataframe"
)

batch_definition = asset.add_batch_definition_whole_dataframe(
    name="complaints_batch"
)


# =========================================================
# 4. EXPECTATION SUITE
# =========================================================

suite = gx.ExpectationSuite(
    name="trusted_complaints_quality"
)


suite.add_expectation(
    gx.expectations.ExpectTableRowCountToEqual(
        value=918
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="ano"
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

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="qtd_recl_total",
        min_value=0,
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="qtd_recl_reguladas_procedentes",
        min_value=0,
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="qtd_recl_reguladas_outras",
        min_value=0,
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="qtd_recl_nao_reguladas",
        min_value=0,
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnPairValuesToBeEqual(
        column_A="_soma_reclamacoes",
        column_B="qtd_recl_total",
    )
)


suite = context.suites.add(
    suite
)


# =========================================================
# 5. VALIDATION DEFINITION
# =========================================================

validation_definition = gx.ValidationDefinition(
    name="validate_trusted_complaints",
    data=batch_definition,
    suite=suite,
)

validation_definition = (
    context.validation_definitions.add(
        validation_definition
    )
)


# =========================================================
# 6. EXECUÇÃO
# =========================================================

result = validation_definition.run(
    batch_parameters={
        "dataframe": complaints
    }
)


# =========================================================
# 7. RESULTADO
# =========================================================

print("\n==========================================")
print(" GREAT EXPECTATIONS — TRUSTED COMPLAINTS")
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


if not result.success:
    sys.exit(1)

sys.exit(0)