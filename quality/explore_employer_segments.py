import pandas as pd


path = "data/trusted_parquet/employer_segments.parquet"

df = pd.read_parquet(path)


print("\n========================================")
print(" EMPLOYER SEGMENTS — EXPLORAÇÃO")
print("========================================")

print(f"\nLinhas: {len(df)}")
print(f"Colunas: {len(df.columns)}")


print("\n=== COLUNAS ===")

for column in df.columns:
    print(
        f"{column}: "
        f"{df[column].dtype}"
    )


print("\n=== PRIMEIRAS LINHAS ===")
print(df.head())


print("\n=== NULOS ===")
print(
    df.isna()
      .sum()
      .sort_values(ascending=False)
)


print("\n=== ESTATÍSTICAS NUMÉRICAS ===")
print(
    df.describe(include="all")
)


# =========================================================
# CAMPOS IMPORTANTES PARA O MART
# =========================================================

important_columns = [
    "segmento",
    "nota_geral",
    "nota_cultura_valores",
    "nota_qualidade_vida",
    "nota_remuneracao_beneficios",
    "pct_recomendam",
    "match_percent",
]


print("\n=== VALORES / FAIXAS IMPORTANTES ===")

for column in important_columns:

    if column not in df.columns:
        continue

    print(f"\n--- {column} ---")

    if pd.api.types.is_numeric_dtype(df[column]):

        print(
            f"Min: {df[column].min()}"
        )

        print(
            f"Max: {df[column].max()}"
        )

    else:

        print(
            df[column]
            .value_counts(dropna=False)
        )