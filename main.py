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

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


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

    engine = ld.get_engine()

    # Send each parquet file to Trusted schema on Postgres DB
    ld.load_dataset("bancos", "bancos", engine)
    ld.load_dataset("complaints_", "complaints", engine)
    ld.load_dataset("employer_segments", "employer_segments", engine)
    ld.load_dataset("employer_cnpj", "employer_cnpj", engine)





if __name__ == "__main__":
    main()
