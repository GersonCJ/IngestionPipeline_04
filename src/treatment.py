from pathlib import Path

import logging
import pandas as pd
from pydantic import BaseModel, ValidationError

from constants import path_strings
from src.schemas import BankValidator, ComplaintsValidator, EmployerSegmentoValidator, EmployerCnpjValidator
from src.sources import read_banks, read_complaints, read_employers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

employer_mapping = {
    "employer_name": "string",
    "reviews_count": "int",
    "culture_count": "int",
    "salaries_count": "int",
    "benefits_count": "int",
    "employer_website": "string",
    "employer_headquarters": "string",
    "employer_founded": "Int64",
    "employer_industry": "string",
    "employer_revenue": "string",
    "url": "string",
    "nota_geral": "float",
    "nota_cultura_valores": "float",
    "nota_diversidade_inclusao": "float",
    "nota_qualidade_vida": "float",
    "nota_alta_lideranca": "float",
    "nota_remuneracao_beneficios": "float",
    "nota_oportunidades_carreira": "float",
    "pct_recomendam": "float",
    "pct_perspectiva_positiva": "float",
    "nome_instituicao": "string",
    "match_percent": "int",
    "employer_revenue_desconhecida": "bool"
}


def validate(raw: pd.DataFrame, model: type[BaseModel]) -> tuple[list[dict], list[dict]]:
    """Validate every row of `raw` against `model`.

    Shared across datasets: row-by-row validation is identical regardless of
    which model or reader produced the input.
    """
    accepted: list[dict] = []
    rejected: list[dict] = []

    for offset, row in enumerate(raw.to_dict("records")):
        try:
            accepted.append(model.model_validate(row).model_dump())
        except ValidationError as error:
            motivo = "; ".join(
                f"{'.'.join(str(loc) for loc in erro['loc'])}: {erro['msg']}"
                for erro in error.errors()
            )
            rejected.append({"_linha": offset + 2, "_motivo": motivo, **row})

    return accepted, rejected


def write_quarantine(rejected: list[dict], dataset: str) -> None:
    """Write rejected rows to `_rejected/{dataset}.csv`. No-op if nothing was rejected."""
    if not rejected:
        return

    target = Path(path_strings.trusted_path) / "_rejected" / f"{dataset}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rejected).to_csv(target, index=False)


def consolidate_quarantine(prefix: str, dataset: str) -> None:
    """Merge per-file `_rejected/{prefix}*.csv` fragments into one `_rejected/{dataset}.csv`.

    No-op if no fragments exist. Each fragment row already carries `_arquivo`
    (the source filename), so merging doesn't lose which file a rejected row
    came from — only the multi-file datasets (Complains) need this.
    """
    rejected_dir = Path(path_strings.trusted_path) / "_rejected"
    fragments = sorted(rejected_dir.glob(f"{prefix}*.csv"))
    if not fragments:
        logger.warning(f"No quarantine fragments found for {dataset}")
        return

    consolidated = pd.concat((pd.read_csv(f, dtype=str) for f in fragments), ignore_index=True)
    consolidated.to_csv(rejected_dir / f"{dataset}.csv", index=False)


def treat_banks(path: Path) -> pd.DataFrame:
    """Validate the Bancos source and write the `bancos` trusted table."""
    raw = read_banks(path)
    accepted, rejected = validate(raw, BankValidator)

    write_quarantine(rejected, "bancos")
    assert len(raw) == len(accepted) + len(rejected), "reconciliation broke for bancos"

    trusted = pd.DataFrame(accepted).astype({
        "segmento": "category",
        "cnpj_base": "string",
        "nome_instituicao": "string",
        "tipo_registro": "category",
        "nome_tem_caractere_invalido": "bool",
    })

    target = Path(path_strings.trusted_path) / "bancos.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    trusted.to_parquet(target, index=False)

    return trusted


def treat_complaints(path: Path) -> pd.DataFrame:
    """Validate the Complains source and write the `complaints` trusted table."""
    try:
        raw = read_complaints(path)
    except pd.errors.EmptyDataError:
        logger.error(f"{path.name} is Empty. Nothing to treat.")
        return

    accepted, rejected = validate(raw, ComplaintsValidator)
    for row in rejected:
        row["_arquivo"] = path.name

    write_quarantine(rejected, f"complaints_{path.name}")
    assert len(raw) == len(accepted) + len(rejected), "reconciliation broke for complaints"

    trusted = pd.DataFrame(accepted).astype({
        "ano": "category",
        "trimestre": "category",
        "categoria": "category",
        "tipo": "category",
        "cnpj_base": "string",
        "instituicao_financeira": "string",
        "indice": "float",
        "qtd_recl_reguladas_procedentes": "int",
        "qtd_recl_reguladas_outras": "int",
        "qtd_recl_nao_reguladas": "int",
        "qtd_recl_total": "int",
        "qtd_clientes_ccs_scr": "Int64",
        "qtd_clientes_ccs": "Int64",
        "qtd_clientes_scr": "Int64"
    })

    target = Path(path_strings.trusted_path) / f"complaints_{path.name}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    trusted.to_parquet(target, index=False)

    return trusted


def treat_employer_segment(path: Path) -> pd.DataFrame:
    """Validate the Empregados source and write the `employers_segment` trusted table."""

    raw = read_employers(path)
    accepted, rejected = validate(raw, EmployerSegmentoValidator)

    write_quarantine(rejected, "employers_segments")
    assert len(raw) == len(accepted) + len(rejected), "reconciliation broke for segments"

    trusted = pd.DataFrame(accepted).astype({
        "segmento": "category",
        **employer_mapping
    })

    target = Path(path_strings.trusted_path) / "employer_segments.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    trusted.to_parquet(target, index=False)

    return trusted


def treat_employer_cnpj(path:Path) -> pd.DataFrame:
    """Validate the Empregados source and write the `employers_cnpj` trusted table."""

    raw = read_employers(path)
    accepted, rejected = validate(raw, EmployerCnpjValidator)

    write_quarantine(rejected, "employers_cnpj")
    assert len(raw) == len(accepted) + len(rejected), "reconciliation broke for Cnpjs"

    trusted = pd.DataFrame(accepted).astype({
        "cnpj_base": "string",
        **employer_mapping,
    })

    target = Path(path_strings.trusted_path) / "employer_cnpj.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    trusted.to_parquet(target, index=False)

    return trusted
    