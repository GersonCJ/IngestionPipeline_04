from pathlib import Path

import pandas as pd


def read_banks(path: Path) -> pd.DataFrame:
    """Read "EnquadramentoInicia_v2.tsv (Bancos)". Returns an all-strings DataFrame.

    Encoding is UTF-8, not latin1: the source already went through a
    cp1252-as-UTF-8 mojibake round-trip before this pipeline ever saw it.
    Decoding as latin1 would not recover the lost accents, only corrupt the
    file differently. BankValidator flags the damage instead of hiding it.
    """
    return pd.read_csv(path, sep="\t", encoding="utf-8", dtype=str, keep_default_na=False)


def read_complaints(path: Path) -> pd.DataFrame:
    """Read 8 .csv complaints. Returns an all-strings DataFrame.

    Every line ends with a trailing ';', so pandas invents a phantom
    "Unnamed: 14" column. Drop it after confirming it's actually empty,
    not by position — a positional drop would silently eat a real column
    if a future source file's shape ever changes.
    """
    raw = pd.read_csv(path, sep=';', encoding="cp1252", dtype=str, keep_default_na=False)

    phantom = "Unnamed: 14"
    if phantom in raw.columns:
        assert raw[phantom].eq("").all(), f"{phantom} is not empty in {path.name} — trap assumption broke"
        raw = raw.drop(columns=phantom)

    return raw


def read_employers(path: Path) -> pd.DataFrame:
    """Read the 2 .csv files. Returns an all-strings DataFrame."""
    return pd.read_csv(path, sep='|', encoding="utf-8", dtype=str, keep_default_na=False)