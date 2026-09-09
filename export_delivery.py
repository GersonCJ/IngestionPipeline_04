"""Exporta a camada Delivery do Postgres para parquet.

Mantido como entrypoint proprio por compatibilidade com o fluxo manual; a
implementacao vive em `src/cli.py`, que e o que o Airflow chama.
"""

from src import cli


def main():
    cli.export()


if __name__ == "__main__":
    main()
