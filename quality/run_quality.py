import subprocess
import sys


# =========================================================
# VALIDAÇÕES DO PIPELINE
# =========================================================

validations = [
    (
        "Trusted - Bancos",
        "quality/validate_trusted_bancos.py",
    ),
    (
        "Trusted - Complaints",
        "quality/validate_trusted_complaints.py",
    ),
    (
        "Trusted - Employer Segments",
        "quality/validate_trusted_employer_segments.py",
    ),
    (
        "Trusted - Employer CNPJ",
        "quality/validate_trusted_employer_cnpj.py",
    ),
    (
        "Delivery / Gold",
        "quality/validate_delivery.py",
    ),
]


# =========================================================
# EXECUÇÃO
# =========================================================

results = []


print("\n")
print("=" * 60)
print(" GREAT EXPECTATIONS — QUALITY PIPELINE")
print("=" * 60)


for name, script in validations:

    print(f"\n>>> Executando: {name}")

    process = subprocess.run(
        [sys.executable, script],
        check=False,
    )

    success = process.returncode == 0

    results.append(
        (name, success)
    )


# =========================================================
# RESUMO
# =========================================================

print("\n")
print("=" * 60)
print(" RESUMO DAS VALIDAÇÕES")
print("=" * 60)


for name, success in results:

    status = "PASSOU" if success else "FALHOU"

    print(
        f"{name:<35} {status}"
    )


# =========================================================
# RESULTADO GERAL
# =========================================================

all_success = all(
    success for _, success in results
)


print("\n" + "=" * 60)

if all_success:

    print(" QUALIDADE GERAL: PASSOU")

else:

    print(" QUALIDADE GERAL: FALHOU")

print("=" * 60)


# =========================================================
# EXIT CODE
# =========================================================

if not all_success:
    sys.exit(1)

sys.exit(0)