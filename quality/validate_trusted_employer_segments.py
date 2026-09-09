import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import great_expectations as gx

from constants import path_strings
from quality import gx_context


def run() -> bool:
    """Validate the Trusted `employer_segments` dataset against the GX suite."""

    # =========================================================
    # 1. CARREGAR DATASET
    # =========================================================

    path = Path(path_strings.trusted_path) / "employer_segments.parquet"

    df = pd.read_parquet(path)

    print(
        f"Employer Segments carregado: "
        f"{len(df)} linhas / {len(df.columns)} colunas"
    )

    # =========================================================
    # 2. GREAT EXPECTATIONS
    # =========================================================

    context = gx_context.build_context()

    batch_definition = gx_context.batch_definition_for(context, "employer_segments")

    # =========================================================
    # 3. EXPECTATION SUITE
    # =========================================================

    suite = gx.ExpectationSuite(
        name="trusted_employer_segments_quality"
    )

    # ---------------------------------------------------------
    # Estrutura geral
    # ---------------------------------------------------------

    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToEqual(
            value=34
        )
    )

    # ---------------------------------------------------------
    # Campos obrigatórios
    # ---------------------------------------------------------

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="employer_name"
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="nome_instituicao"
        )
    )

    # ---------------------------------------------------------
    # Segmentação bancária
    # ---------------------------------------------------------

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

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pct_recomendam",
            min_value=0,
            max_value=100,
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pct_perspectiva_positiva",
            min_value=0,
            max_value=100,
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="match_percent",
            min_value=0,
            max_value=100,
        )
    )

    # =========================================================
    # 4. REGISTRAR SUITE
    # =========================================================

    result = gx_context.run_validation(
        context,
        name="validate_trusted_employer_segments",
        suite=suite,
        batch_definition=batch_definition,
        df=df,
    )

    # =========================================================
    # 7. RESULTADO
    # =========================================================

    gx_context.publish_docs(context)

    return gx_context.report("EMPLOYER SEGMENTS", result)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
