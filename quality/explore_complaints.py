from pathlib import Path

import pandas as pd


trusted_path = Path("data/trusted_parquet")

files = sorted(
    trusted_path.glob("complaints_*.parquet")
)

print("\n=== ARQUIVOS ENCONTRADOS ===")

for file in files:
    print(file.name)


dataframes = [
    pd.read_parquet(file)
    for file in files
]

complaints = pd.concat(
    dataframes,
    ignore_index=True
)


print("\n=== DATASET CONSOLIDADO ===")
print(f"Arquivos: {len(files)}")
print(f"Linhas: {len(complaints)}")
print(f"Colunas: {len(complaints.columns)}")

print("\n=== COLUNAS ===")
for column in complaints.columns:
    print(column)

print("\n=== AMOSTRA ===")
print(complaints.head())