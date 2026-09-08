import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import great_expectations as gx

from constants import path_strings


def run() -> bool:
    """Validate the Trusted `bancos` dataset against the Great Expectations suite."""

    # =========================================================
    # 1. CARREGAR DATASET
    # =========================================================

    path = Path(path_strings.trusted_path) / "bancos.parquet"

    df = pd.read_parquet(path)

    print(f"Bancos carregado: {len(df)} linhas / {len(df.columns)} colunas")

    # =========================================================
    # 2. GREAT EXPECTATIONS
    # =========================================================

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name="bancos_runtime")

    asset = data_source.add_dataframe_asset(name="bancos_dataframe")

    batch_definition = asset.add_batch_definition_whole_dataframe(name="bancos_batch")

    # =========================================================
    # 3. EXPECTATION SUITE
    # =========================================================

    suite = gx.ExpectationSuite(name="trusted_bancos_quality")

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="segmento",
            value_set=[
                "S1",
                "S2",
                "S3",
                "S4",
                "S5",
            ],
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="cnpj_base")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="cnpj_base",
            regex=r"^\d{8}$",
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="nome_instituicao")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="tipo_registro",
            value_set=["PRUDENCIAL", "INSTITUICAO"],
        )
    )

    suite = context.suites.add(suite)

    # =========================================================
    # 4. VALIDATION DEFINITION
    # =========================================================

    validation_definition = gx.ValidationDefinition(
        name="validate_trusted_bancos",
        data=batch_definition,
        suite=suite,
    )

    validation_definition = context.validation_definitions.add(validation_definition)

    # =========================================================
    # 5. EXECUTAR
    # =========================================================

    result = validation_definition.run(batch_parameters={"dataframe": df})

    # =========================================================
    # 6. RESULTADO
    # =========================================================

    print("\n========================================")
    print(" GREAT EXPECTATIONS — TRUSTED BANCOS")
    print("========================================")

    print(f"Resultado geral: {'PASSOU' if result.success else 'FALHOU'}")
    print(f"Expectations avaliadas: {result.statistics['evaluated_expectations']}")
    print(f"Expectations aprovadas: {result.statistics['successful_expectations']}")
    print(f"Expectations reprovadas: {result.statistics['unsuccessful_expectations']}")
    print(f"Taxa de sucesso: {result.statistics['success_percent']:.2f}%")

    return result.success


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
