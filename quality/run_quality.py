"""Executor de todas as suites de qualidade, do Trusted ao Delivery.

Continua servindo como ponto de entrada manual (`python quality/run_quality.py`)
e devolve exit code 0/1. No pipeline orquestrado o Airflow chama as camadas
separadamente — `src.cli quality-trusted` e `src.cli quality-delivery` — para
que uma reprovação no Trusted interrompa a DAG antes da carga no Postgres.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quality.validate_delivery import run as validate_delivery
from quality.validate_trusted_bancos import run as validate_bancos
from quality.validate_trusted_complaints import run as validate_complaints
from quality.validate_trusted_employer_cnpj import run as validate_employer_cnpj
from quality.validate_trusted_employer_segments import run as validate_employer_segments


VALIDATIONS = [
    ("Trusted - Bancos", validate_bancos),
    ("Trusted - Complaints", validate_complaints),
    ("Trusted - Employer Segments", validate_employer_segments),
    ("Trusted - Employer CNPJ", validate_employer_cnpj),
    ("Delivery / Gold", validate_delivery),
]


def main() -> bool:
    print("\n")
    print("=" * 60)
    print(" GREAT EXPECTATIONS — QUALITY PIPELINE")
    print("=" * 60)

    results = []

    for name, validation in VALIDATIONS:
        print(f"\n>>> Executando: {name}")
        results.append((name, validation()))

    print("\n")
    print("=" * 60)
    print(" RESUMO DAS VALIDAÇÕES")
    print("=" * 60)

    for name, success in results:
        print(f"{name:<35} {'PASSOU' if success else 'FALHOU'}")

    all_success = all(success for _, success in results)

    print("\n" + "=" * 60)
    print(f" QUALIDADE GERAL: {'PASSOU' if all_success else 'FALHOU'}")
    print("=" * 60)

    return all_success


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
