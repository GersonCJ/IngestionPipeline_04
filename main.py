from constants import path_strings
from src.treatment import (
    consolidate_quarantine,
    treat_banks,
    treat_complaints,
    treat_employer_cnpj,
    treat_employer_segment,
)
from dotenv import load_dotenv
from pathlib import Path
import src.load as ld
import logging

from quality.validate_trusted_bancos import run as validate_bancos_quality
from quality.validate_trusted_complaints import run as validate_complaints_quality
from quality.validate_trusted_employer_cnpj import run as validate_employer_cnpj_quality
from quality.validate_trusted_employer_segments import run as validate_employer_segments_quality

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def run_trusted_quality_gate() -> None:
    """Run the Great Expectations suites on the Trusted layer.

    Acts as a gate between the Trusted parquet files and Postgres: if any
    dataset fails its expectations, the pipeline stops before loading it.
    """
    checks = {
        "Trusted - Bancos": validate_bancos_quality,
        "Trusted - Complaints": validate_complaints_quality,
        "Trusted - Employer Segments": validate_employer_segments_quality,
        "Trusted - Employer CNPJ": validate_employer_cnpj_quality,
    }

    results = {name: check() for name, check in checks.items()}
    failed = [name for name, passed in results.items() if not passed]

    if failed:
        raise RuntimeError(f"Great Expectations quality gate failed for: {', '.join(failed)}")

    logger.info("Great Expectations quality gate passed for all Trusted datasets.")


def main():
    bancos_path = Path(path_strings.bronze_path) / "Bancos" / "EnquadramentoInicia_v2.tsv"
    treat_banks(bancos_path)

    complaints_path = Path(path_strings.bronze_path) / "Complains"
    for file in complaints_path.iterdir():
        if file.is_file():
            treat_complaints(file)
    consolidate_quarantine("complaints_", "complaints")

    employers_cnpj = Path(path_strings.bronze_path) / "Empregados" / "glassdoor_consolidado_join_match_less_v2.csv"
    treat_employer_cnpj(employers_cnpj)

    employers_segments = Path(path_strings.bronze_path) / "Empregados" / "glassdoor_consolidado_join_match_v2.csv"
    treat_employer_segment(employers_segments)

    run_trusted_quality_gate()

    engine = ld.get_engine()

    # Send each parquet file to Trusted schema on Postgres DB
    ld.load_dataset("bancos", "bancos", engine)
    ld.load_dataset("complaints_", "complaints", engine)
    ld.load_dataset("employer_segments", "employer_segments", engine)
    ld.load_dataset("employer_cnpj", "employer_cnpj", engine)





if __name__ == "__main__":
    main()
