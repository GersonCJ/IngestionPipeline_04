import great_expectations as gx
import sys

# =========================================================
# 1. CARREGAR GX
# =========================================================

context = gx.get_context(
    mode="file",
    project_root_dir="quality/gx_project"
)


# =========================================================
# 2. RECUPERAR DADOS CONFIGURADOS
# =========================================================

data_source = context.data_sources.get(
    "trusted_filesystem"
)

asset = data_source.get_asset(
    "bancos_parquet"
)

batch_definition = asset.get_batch_definition(
    "bancos_batch"
)


# =========================================================
# 3. EXPECTATION SUITE
# =========================================================

suite = gx.ExpectationSuite(
    name="trusted_bancos_quality"
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="segmento",
       value_set=[
    "S1",
    "S2",
    "S3",
    "S4",
    "S5",
]
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="cnpj_base"
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="cnpj_base",
        regex=r"^\d{8}$",
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="nome_instituicao"
    )
)


suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="tipo_registro",
        value_set=["PRUDENCIAL", "INSTITUICAO"],
    )
)


suite = context.suites.add_or_update(suite)


# =========================================================
# 4. VALIDATION DEFINITION
# =========================================================

validation_definition = gx.ValidationDefinition(
    name="validate_trusted_bancos",
    data=batch_definition,
    suite=suite,
)

validation_definition = context.validation_definitions.add_or_update(
    validation_definition
)


# =========================================================
# 5. EXECUTAR
# =========================================================

result = validation_definition.run()


# =========================================================
# 6. RESULTADO
# =========================================================

print("\n========================================")
print(" GREAT EXPECTATIONS — TRUSTED BANCOS")
print("========================================")

print(
    f"Resultado geral: "
    f"{'PASSOU' if result.success else 'FALHOU'}"
)

print(
    f"Expectations avaliadas: "
    f"{result.statistics['evaluated_expectations']}"
)

print(
    f"Expectations aprovadas: "
    f"{result.statistics['successful_expectations']}"
)

print(
    f"Expectations reprovadas: "
    f"{result.statistics['unsuccessful_expectations']}"
)

print(
    f"Taxa de sucesso: "
    f"{result.statistics['success_percent']:.2f}%"
)

# =========================================================
# 8. EXIT CODE PARA FUTURA ORQUESTRAÇÃO
# =========================================================

if not result.success:
    sys.exit(1)

sys.exit(0)