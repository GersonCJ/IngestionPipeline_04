"""Etapas do pipeline expostas como subcomandos.

O `main.py` executa a cadeia inteira num processo só, o que serve para rodar o
pipeline a mão. O Airflow precisa do oposto: cada etapa como um processo
proprio, para ter log, retry e status individuais no grafo da DAG. Este modulo
e essa fronteira — ele nao reimplementa nada, so isola os passos que ja existem
em `src.treatment`, `src.load` e `quality/`.

    uv run python -m src.cli transform
    uv run python -m src.cli quality-trusted
    uv run python -m src.cli load
    uv run python -m src.cli export
    uv run python -m src.cli quality-delivery
    uv run python -m src.cli catalog

Cada etapa sai com codigo 0 em caso de sucesso e levanta excecao (codigo != 0)
em caso de falha — e assim que o DockerOperator decide se a task passou.
"""

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from constants import path_strings
import src.load as ld
from src.treatment import (
    consolidate_quarantine,
    treat_banks,
    treat_complaints,
    treat_employer_cnpj,
    treat_employer_segment,
)

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def transform() -> None:
    """Raw -> Trusted: valida linha a linha com Pydantic e grava os parquets."""
    bronze = Path(path_strings.bronze_path)

    treat_banks(bronze / "Bancos" / "EnquadramentoInicia_v2.tsv")

    complaints_path = bronze / "Complains"
    for file in complaints_path.iterdir():
        if file.is_file():
            treat_complaints(file)
    consolidate_quarantine("complaints_", "complaints")

    treat_employer_cnpj(
        bronze / "Empregados" / "glassdoor_consolidado_join_match_less_v2.csv"
    )
    treat_employer_segment(
        bronze / "Empregados" / "glassdoor_consolidado_join_match_v2.csv"
    )

    logger.info("Camada Trusted gerada em %s", path_strings.trusted_path)


def quality_trusted() -> None:
    """Gate de qualidade da camada Trusted.

    Roda antes da carga no Postgres: se um dataset reprovar, a excecao aqui
    impede que ele chegue ao banco e que o dbt construa a Delivery em cima.
    """
    from quality.validate_trusted_bancos import run as validate_bancos
    from quality.validate_trusted_complaints import run as validate_complaints
    from quality.validate_trusted_employer_cnpj import run as validate_employer_cnpj
    from quality.validate_trusted_employer_segments import run as validate_employer_segments

    checks = {
        "Trusted - Bancos": validate_bancos,
        "Trusted - Complaints": validate_complaints,
        "Trusted - Employer Segments": validate_employer_segments,
        "Trusted - Employer CNPJ": validate_employer_cnpj,
    }

    results = {name: check() for name, check in checks.items()}
    failed = [name for name, passed in results.items() if not passed]

    if failed:
        raise RuntimeError(
            f"Great Expectations quality gate failed for: {', '.join(failed)}"
        )

    logger.info("Great Expectations quality gate passed for all Trusted datasets.")


def load() -> None:
    """Trusted parquet -> Postgres (schema trusted_atv4)."""
    engine = ld.get_engine()

    ld.load_dataset("bancos", "bancos", engine)
    ld.load_dataset("complaints_", "complaints", engine)
    ld.load_dataset("employer_segments", "employer_segments", engine)
    ld.load_dataset("employer_cnpj", "employer_cnpj", engine)


def export() -> None:
    """Delivery (Postgres) -> parquet, exigencia do enunciado."""
    engine = ld.get_engine()
    ld.export_table_to_parquet("delivery_reclamacoes_satisfacao", "delivery_atv4", engine)


def quality_delivery() -> None:
    """Gate de qualidade da camada Delivery, incluindo a reconciliacao de linhas."""
    from quality.validate_delivery import run as validate_delivery

    if not validate_delivery():
        raise RuntimeError("Great Expectations quality gate failed for: Delivery / Gold")

    logger.info("Great Expectations quality gate passed for Delivery.")


def catalog() -> None:
    """Publica o catalogo do Postgres no OpenMetadata."""
    from src.catalog import run as run_catalog

    run_catalog()


STEPS = {
    "transform": transform,
    "quality-trusted": quality_trusted,
    "load": load,
    "export": export,
    "quality-delivery": quality_delivery,
    "catalog": catalog,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="src.cli",
        description="Executa uma etapa isolada do pipeline medalhao.",
    )
    parser.add_argument("step", choices=list(STEPS), help="etapa a executar")

    args = parser.parse_args(argv)

    logger.info("Executando etapa: %s", args.step)
    STEPS[args.step]()
    logger.info("Etapa concluida: %s", args.step)


if __name__ == "__main__":
    main(sys.argv[1:])
