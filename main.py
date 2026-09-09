"""Execucao manual do pipeline inteiro, do raw ao gate da camada Trusted.

No fluxo orquestrado quem chama cada etapa e o Airflow, uma por task
(`src/cli.py`). Este arquivo continua sendo o atalho para rodar tudo de uma vez
sem orquestrador:

    docker compose --profile build run --rm app
"""

from src import cli


def main():
    cli.transform()
    cli.quality_trusted()
    cli.load()


if __name__ == "__main__":
    main()
