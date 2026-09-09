import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import great_expectations as gx

from great_expectations.expectations.row_conditions import Column

from constants import path_strings
from quality import gx_context


def run() -> bool:
    """Validate the Delivery/Gold dataset and reconcile it against Trusted complaints."""

    # =========================================================
    # 1. CARREGAR DELIVERY
    # =========================================================

    delivery_path = (
        Path(path_strings.delivery_path) / "delivery_reclamacoes_satisfacao.parquet"
    )

    delivery = pd.read_parquet(delivery_path)

    print(
        f"Delivery carregada: "
        f"{len(delivery)} linhas / {len(delivery.columns)} colunas"
    )

    # =========================================================
    # 2. COMPLAINTS PARA RECONCILIAÇÃO
    # =========================================================

    trusted_path = Path(path_strings.trusted_path)

    complaint_files = sorted(
        trusted_path.glob("complaints_*.parquet")
    )

    if not complaint_files:
        print("ERRO: nenhum arquivo complaints_*.parquet encontrado.")
        return False

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

    context = gx_context.build_context()

    batch_definition = gx_context.batch_definition_for(context, "delivery")

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

    result = gx_context.run_validation(
        context,
        name="validate_delivery",
        suite=suite,
        batch_definition=batch_definition,
        df=delivery,
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

    gx_context.publish_docs(context)

    # A reconciliação entre camadas não é uma expectation: ela compara dois
    # datasets distintos, então entra no resultado por fora da suite.
    return gx_context.report("DELIVERY / GOLD", result) and reconciliation_success


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
