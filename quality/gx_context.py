"""Contexto compartilhado do Great Expectations para as suites do pipeline.

Os validadores usavam `gx.get_context(mode="ephemeral")`: nada sobrevivia a
execucao e o volume `gx_docs` ficava sem uso. Aqui o contexto e persistente
(`/app/gx`, ver constants.path_strings.gx_path), o que da historico de
validacoes e Data Docs navegaveis em http://localhost:8182.

Contexto persistente cobra um preco: `add_pandas`, `add_dataframe_asset` e
`add_batch_definition_whole_dataframe` levantam erro se o objeto ja existe, e
so a partir da segunda execucao. Por isso todo acesso aqui e fetch-first,
caindo no `add_*` apenas no `LookupError`.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import great_expectations as gx

from constants import path_strings


def build_context():
    """Abre (ou cria) o Data Context em disco compartilhado pelas suites."""
    return gx.get_context(mode="file", project_root_dir=path_strings.gx_path)


def batch_definition_for(context, dataset: str):
    """Devolve a batch definition de `dataset`, criando o que faltar.

    Cada dataset tem data source, asset e batch definition proprios, nomeados
    a partir dele — `bancos` vira `bancos_runtime` / `bancos_dataframe` /
    `bancos_batch`, mantendo os nomes que as suites ja usavam.
    """
    source_name = f"{dataset}_runtime"
    asset_name = f"{dataset}_dataframe"
    batch_name = f"{dataset}_batch"

    try:
        data_source = context.data_sources.get(source_name)
    except LookupError:
        # add_or_update_pandas substitui o data source inteiro, derrubando os
        # assets pendurados nele — por isso so e chamado quando nao existe.
        data_source = context.data_sources.add_or_update_pandas(name=source_name)

    try:
        asset = data_source.get_asset(asset_name)
    except LookupError:
        asset = data_source.add_dataframe_asset(name=asset_name)

    try:
        return asset.get_batch_definition(batch_name)
    except LookupError:
        return asset.add_batch_definition_whole_dataframe(name=batch_name)


def run_validation(context, name: str, suite, batch_definition, df):
    """Registra suite e validation definition (idempotente) e executa em `df`."""
    suite = context.suites.add_or_update(suite)

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=name, data=batch_definition, suite=suite)
    )

    return validation_definition.run(batch_parameters={"dataframe": df})


def report(title: str, result) -> bool:
    """Imprime o resumo da validacao e devolve se ela passou."""
    print("\n========================================")
    print(f" GREAT EXPECTATIONS — {title}")
    print("========================================")

    print(f"Resultado geral: {'PASSOU' if result.success else 'FALHOU'}")
    print(f"Expectations avaliadas: {result.statistics['evaluated_expectations']}")
    print(f"Expectations aprovadas: {result.statistics['successful_expectations']}")
    print(f"Expectations reprovadas: {result.statistics['unsuccessful_expectations']}")
    print(f"Taxa de sucesso: {result.statistics['success_percent']:.2f}%")

    return result.success


def publish_docs(context) -> None:
    """Gera os Data Docs e um index na raiz do volume que aponta para eles.

    O nginx serve a raiz de `gx_docs`, mas o GE escreve os docs em
    `gx/uncommitted/data_docs/local_site/`. O redirect evita ter que decorar
    esse caminho ao abrir http://localhost:8182.
    """
    context.build_data_docs()

    root = Path(path_strings.gx_path)
    docs_index = root / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"

    if docs_index.exists():
        destination = docs_index.relative_to(root).as_posix()
        (root / "index.html").write_text(
            f'<!doctype html><meta http-equiv="refresh" content="0; url={destination}">'
            f'<a href="{destination}">Great Expectations Data Docs</a>',
            encoding="utf-8",
        )
