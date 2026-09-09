import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import great_expectations as gx

from constants import path_strings
from quality import gx_context


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

    context = gx_context.build_context()

    batch_definition = gx_context.batch_definition_for(context, "bancos")

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

    # =========================================================
    # 4. EXECUTAR
    # =========================================================

    result = gx_context.run_validation(
        context,
        name="validate_trusted_bancos",
        suite=suite,
        batch_definition=batch_definition,
        df=df,
    )

    # =========================================================
    # 5. RESULTADO
    # =========================================================

    gx_context.publish_docs(context)

    return gx_context.report("TRUSTED BANCOS", result)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
