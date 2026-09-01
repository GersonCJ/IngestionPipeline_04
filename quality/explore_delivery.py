from pathlib import Path

import pandas as pd


# =========================================================
# 1. DELIVERY / GOLD
# =========================================================

delivery_path = Path(
    "data/delivered_gold/delivery_reclamacoes_satisfacao.parquet"
)

delivery = pd.read_parquet(delivery_path)


print("\n========================================")
print(" DELIVERY / GOLD — EXPLORAÇÃO")
print("========================================")

print(f"\nLinhas: {len(delivery)}")
print(f"Colunas: {len(delivery.columns)}")


print("\n=== COLUNAS ===")

for column in delivery.columns:
    print(f"{column}: {delivery[column].dtype}")


print("\n=== PRIMEIRAS LINHAS ===")
print(delivery.head())


print("\n=== NULOS ===")

print(
    delivery.isna()
    .sum()
    .sort_values(ascending=False)
)


print("\n=== FAIXAS IMPORTANTES ===")

numeric_columns = [
    "ano",
    "trimestre",
    "qtd_recl_total",
    "qtd_recl_reguladas_procedentes",
    "nota_geral",
    "nota_qualidade_vida",
    "nota_remuneracao_beneficios",
    "pct_recomendam",
    "match_percent",
]

for column in numeric_columns:

    if column not in delivery.columns:
        continue

    print(f"\n--- {column} ---")

    print(f"Min: {delivery[column].min()}")
    print(f"Max: {delivery[column].max()}")


# =========================================================
# 2. MATCH COM EMPREGADOS
# =========================================================

matches = delivery["nota_geral"].notna().sum()

print("\n=== MATCH EMPREGADOS ===")

print(f"Linhas com nota_geral: {matches}")

print(
    f"Taxa de match: "
    f"{matches / len(delivery) * 100:.2f}%"
)