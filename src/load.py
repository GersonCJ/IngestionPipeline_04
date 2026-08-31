import logging
import os
import pandas as pd
from constants import path_strings
from pathlib import Path
from sqlalchemy import Engine, create_engine, inspect, text

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Build the Postgres engine from the DB_* environment variables."""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")


def push_to_db(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str = "trusted_atv4",
    if_exists: str = "append",
) -> None:
    """Push a DataFrame to `schema.table_name` in Postgres.

    `if_exists` defaults to "append" since a logical trusted table (e.g.
    Complains) may be loaded across several calls, one per source fragment —
    only the first call for a given table should use "replace".

    "replace" truncates the table's rows instead of dropping and recreating
    it. dbt builds views directly on top of these tables (e.g. stg_bancos on
    trusted_atv4.bancos) — a DROP TABLE fails once such a view exists
    ("DependentObjectsStillExist"), and a CASCADE drop would silently destroy
    the dbt layer on every re-ingest. Truncating leaves the table object (and
    anything depending on it) untouched; only its rows change.
    """
    logger.info(f"Uploading {len(df)} rows to {schema}.{table_name} (if_exists={if_exists})...")
    try:
        if if_exists == "replace" and inspect(engine).has_table(table_name, schema=schema):
            with engine.begin() as conn:
                conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}"'))
            if_exists = "append"

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=10000,
        )
    except Exception:
        logger.exception(f"Failed to load {schema}.{table_name}")
        raise
    logger.info(f"Upload to {schema}.{table_name} complete.")


def load_dataset(prefix: str, table_name: str, engine, trusted_dir: Path = Path(path_strings.trusted_path)) -> None:
    fragments = sorted(trusted_dir.glob(f"{prefix}*.parquet"))
    if not fragments:
        logger.warning(f"No parquet fragments found for {table_name}")
        return
    df = pd.concat((pd.read_parquet(f) for f in fragments), ignore_index=True)
    push_to_db(df, table_name, engine, if_exists="replace")

def run_query(sql_query: str, engine: Engine, params=None) -> pd.DataFrame:
    """Run SQL query to retrieve data from database"""
    with engine.connect() as conn:
        return pd.read_sql(sql_query, conn, params=params)


def export_table_to_parquet(
    table_name: str,
    schema: str,
    engine: Engine,
    out_dir: Path = Path(path_strings.delivery_path),
) -> Path:
    """Export a full Postgres table to `{out_dir}/{table_name}.parquet`."""
    df = run_query(f"select * from {schema}.{table_name}", engine)

    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{table_name}.parquet"
    df.to_parquet(target, index=False)
    logger.info(f"Exported {schema}.{table_name} ({len(df)} rows) to {target}")

    return target