import logging

from dotenv import load_dotenv

import src.load as ld

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main():
    engine = ld.get_engine()
    ld.export_table_to_parquet("delivery_reclamacoes_satisfacao", "delivery_atv4", engine)


if __name__ == "__main__":
    main()
