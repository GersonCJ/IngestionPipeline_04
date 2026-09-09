from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import great_expectations as gx

from constants import path_strings
from quality import gx_context


def run() -> bool:
    """Validate the Trusted `complaints` dataset (all quarterly fragments) against the GX suite."""

    # =========================================================
    # 1. CARREGAR COMPLAINTS
    # =========================================================

    trusted_path = Path(path_strings.trusted_path)

    files = sorted(
        trusted_path.glob("complaints_*.parquet")
    )

    if not files:
        print("ERRO: nenhum arquivo complaints_*.parquet encontrado.")
        return False

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

    context = gx_context.build_context()

    batch_definition = gx_context.batch_definition_for(context, "complaints")

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

    result = gx_context.run_validation(
        context,
        name="validate_trusted_complaints",
        suite=suite,
        batch_definition=batch_definition,
        df=complaints,
    )

    # =========================================================
    # 7. RESULTADO
    # =========================================================

    gx_context.publish_docs(context)

    return gx_context.report("TRUSTED COMPLAINTS", result)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
