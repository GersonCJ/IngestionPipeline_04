import great_expectations as gx

context = gx.get_context(mode="ephemeral")

batch = context.data_sources.pandas_default.read_parquet(
    "data/trusted_parquet/bancos.parquet"
)

expectations = [
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="segmento",
        value_set=["S1", "S2", "S3", "S4", "S5"],
    ),

    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="cnpj_base",
    ),

    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="cnpj_base",
        regex=r"^\d{8}$",
    ),

    gx.expectations.ExpectColumnValuesToNotBeNull(
        column="nome_instituicao",
    ),

    gx.expectations.ExpectColumnValuesToBeInSet(
        column="tipo_registro",
        value_set=["PRUDENCIAL", "INSTITUICAO"],
    ),
]

print("\n=== GREAT EXPECTATIONS — TRUSTED BANCOS ===")

for expectation in expectations:

    result = batch.validate(expectation)

    status = "PASSOU" if result["success"] else "FALHOU"

    print(
        f"{expectation.__class__.__name__}: {status}"
    )