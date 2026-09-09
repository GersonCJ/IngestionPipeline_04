# Como rodar o projeto

Passo a passo para subir a stack e executar o pipeline medalhão do zero.

Cada comando aparece em duas versões: **PowerShell** e **Bash** (Git Bash no
Windows, ou WSL/Linux). Use a do terminal que você estiver. Onde os dois são
idênticos — o caso da maioria dos `docker compose` — as duas versões aparecem
mesmo assim, para você poder copiar sem pensar.

> **Git Bash:** ele converte caminhos que começam com `/` em caminhos do
> Windows. Se precisar passar um caminho absoluto de dentro do container (por
> exemplo `/var/run/docker.sock`), prefixe o comando com `MSYS_NO_PATHCONV=1`
> ou dobre a primeira barra (`//var/run/docker.sock`).

Se algo der errado, vá direto para [Quando algo falha](#quando-algo-falha) —
os erros mais prováveis estão mapeados lá.

---

## 0. Antes de começar

O Docker Desktop precisa estar rodando. Confira:

```powershell
docker info --format "RAM={{.MemTotal}} CPUs={{.NCPU}}"
```

```bash
docker info --format 'RAM={{.MemTotal}} CPUs={{.NCPU}}'
```

A stack sobe Airflow, Postgres, OpenMetadata e Elasticsearch. **Reserve pelo
menos 8 GB** ao Docker Desktop (Settings → Resources → Memory). Com menos que
isso, o Elasticsearch entra em GC thrash e o OpenMetadata nunca fica saudável.

O primeiro `up` baixa cerca de 5 GB de imagens (Airflow 3.3.1, OpenMetadata
2.0.1, Elasticsearch 9.3.0). Conte com alguns minutos.

---

## 1. Clonar o repositório

O projeto fica em `C:\Users\<seu-usuario>\IA\Workspace`. Crie o diretório se ele
ainda não existir — os comandos abaixo criam a árvore inteira de uma vez e não
reclamam se ela já existir:

```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\IA\Workspace" | Out-Null
Set-Location "$env:USERPROFILE\IA\Workspace"
```

```bash
mkdir -p ~/IA/Workspace
cd ~/IA/Workspace
```

Clone e entre no projeto:

```powershell
git clone https://github.com/GersonCJ/IngestionPipeline_04.git
Set-Location IngestionPipeline_04
```

```bash
git clone https://github.com/GersonCJ/IngestionPipeline_04.git
cd IngestionPipeline_04
```

Todos os comandos daqui em diante assumem que você está na raiz do repositório.

---

## 2. Preparar o `.env`

```powershell
Copy-Item .env.env .env
```

```bash
cp .env.env .env
```

Agora a parte que mais causa problema: a variável **`HOST_PROJECT_DIR`**. Ela é
o caminho deste repositório **como o daemon do Docker o enxerga**.

O Airflow cria cada task como um container irmão, e os volumes que ele pede são
resolvidos pelo daemon — que no Docker Desktop roda numa VM, onde o disco `C:`
aparece sob `/run/desktop/mnt/host/c/`. Não use `C:\...` aqui: o daemon não
reclama, apenas cria um diretório vazio, e as tasks passam sem processar nada.
É o erro mais silencioso de toda a stack.

Em vez de montar o caminho à mão, gere o valor a partir do diretório atual:

```powershell
$drive = $PWD.Drive.Name.ToLower()
$rest  = $PWD.Path.Substring(2) -replace '\\','/'
"HOST_PROJECT_DIR=/run/desktop/mnt/host/$drive$rest"
```

```bash
echo "HOST_PROJECT_DIR=/run/desktop/mnt/host$(pwd)"
```

Copie a linha impressa para o `.env`, substituindo a que já está lá. Seguindo o
caminho sugerido no passo 1, ela fica assim:

```env
HOST_PROJECT_DIR=/run/desktop/mnt/host/c/Users/<seu-usuario>/IA/Workspace/IngestionPipeline_04
```

Deixe `OM_JWT_TOKEN` vazio por enquanto — ele é preenchido no passo 7.

---

## 3. Limpar o estado anterior

Pule este passo se você acabou de clonar o repositório numa máquina que nunca
rodou o projeto. Ele é necessário quando existem volumes de uma versão anterior
da stack, porque três coisas mudaram:

- o Postgres foi pinado na `17.11`, e o ponto de montagem do volume mudou;
- o `init.sql` (que cria os schemas e os databases do Airflow e do
  OpenMetadata) só roda em volume vazio;
- o OpenMetadata saltou da `1.3.2` para a `2.0.1`, e não há caminho de upgrade
  in-place entre essas versões — o schema precisa ser recriado do zero.

```powershell
docker compose down -v --remove-orphans
```

```bash
docker compose down -v --remove-orphans
```

Opcionalmente, remova as imagens da stack antiga, que não são mais usadas:

```powershell
docker image rm customized-airflow:latest openmetadata/server:1.3.2 ingestion_atv4:local dbt_atv4:local
```

```bash
docker image rm customized-airflow:latest openmetadata/server:1.3.2 ingestion_atv4:local dbt_atv4:local
```

---

## 4. Construir as imagens das tasks

```powershell
docker compose --profile build build
```

```bash
docker compose --profile build build
```

Isso gera as duas imagens que a DAG instancia:

| Imagem | Conteúdo |
|---|---|
| `ingestion_atv4` | Python 3.14 + Pydantic + Great Expectations + SDK do OpenMetadata |
| `dbt_atv4` | dbt-postgres, com o projeto dbt e os profiles embutidos |

O Airflow **não** recebe pandas, GE nem dbt — ele só orquestra.

---

## 5. Subir a stack

```powershell
docker compose up -d
```

```bash
docker compose up -d
```

A ordem de boot é encadeada, e vale acompanhá-la:

```powershell
docker compose ps
```

```bash
docker compose ps
```

O que esperar, nesta ordem:

1. `postgres-db` → `healthy`
2. `elasticsearch` → `healthy` (é o mais demorado, até ~2 min)
3. `execute-migrate-all` → **exited (0)** — é um container de bootstrap, ele
   termina mesmo; sair com 0 é o resultado correto
4. `openmetadata-server` → `healthy`
5. `airflow-init` → **exited (0)**
6. `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` → `healthy`

Os dois containers de bootstrap somem de `docker compose ps` depois de
terminarem. Para vê-los:

```powershell
docker compose ps -a
```

```bash
docker compose ps -a
```

Para acompanhar um serviço específico:

```powershell
docker compose logs -f openmetadata-server
```

```bash
docker compose logs -f openmetadata-server
```

---

## 6. Conferir que o Airflow leu a DAG

```powershell
docker compose exec --user airflow airflow-scheduler airflow dags list
```

```bash
docker compose exec --user airflow airflow-scheduler airflow dags list
```

Deve aparecer `atv4_medallion_end_to_end`.

O `--user airflow` não é opcional: o serviço roda como `root` (para acessar o
socket do Docker), mas o Airflow está instalado no *user-site* do usuário
`airflow`. Sem ele, o comando falha com `ModuleNotFoundError: No module named
'airflow'` — mesmo com o scheduler perfeitamente saudável.

Se aparecer um erro de import em vez da DAG:

```powershell
docker compose exec --user airflow airflow-scheduler airflow dags list-import-errors
```

```bash
docker compose exec --user airflow airflow-scheduler airflow dags list-import-errors
```

A DAG **falha o parse de propósito** quando `HOST_PROJECT_DIR` não está
definida — é melhor um erro visível do que tasks verdes que não processaram
nada. Se for esse o caso, volte ao passo 2 e depois:

```powershell
docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor airflow-apiserver
```

```bash
docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor airflow-apiserver
```

As interfaces já devem estar no ar:

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8081 | `airflow` / `airflow` |
| OpenMetadata | http://localhost:8585 | ver passo 7 |
| dbt docs | http://localhost:8181 | — |
| Data Docs (Great Expectations) | http://localhost:8182 | — |

As duas últimas ficam vazias até a primeira execução da DAG — elas servem
artefatos que o pipeline ainda não gerou.

---

## 7. Pegar o token do OpenMetadata

A última task da DAG publica o catálogo no OpenMetadata, e para isso precisa do
token do bot de ingestão.

1. Abra http://localhost:8585.
2. Faça login. O usuário administrador é `admin@open-metadata.org`. Se a senha
   padrão não funcionar, ela é gerada no primeiro boot e sai no log:

   ```powershell
   docker compose logs openmetadata-server | Select-String -Pattern "admin"
   ```

   ```bash
   docker compose logs openmetadata-server | grep -i admin
   ```

   O cadastro próprio também está habilitado, então criar uma conta é uma saída
   válida.
3. Vá em **Settings → Bots → `ingestion-bot`** e copie o token.
4. Cole no `.env`:

   ```env
   OM_JWT_TOKEN=<token copiado>
   ```
5. Repropague para os serviços do Airflow:

   ```powershell
   docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor
   ```

   ```bash
   docker compose up -d --force-recreate airflow-scheduler airflow-dag-processor
   ```

Se você pular este passo, tudo roda normalmente e **apenas a última task**
(`openmetadata_catalog`) falha, com uma mensagem dizendo exatamente isso.

---

## 8. Rodar o pipeline

Pela interface: abra http://localhost:8081, entre em
`atv4_medallion_end_to_end` e clique em **Trigger**.

Ou pela linha de comando:

```powershell
docker compose exec --user airflow airflow-scheduler airflow dags trigger atv4_medallion_end_to_end
```

```bash
docker compose exec --user airflow airflow-scheduler airflow dags trigger atv4_medallion_end_to_end
```

A DAG é uma cadeia linear de nove tasks:

```
raw_to_trusted → gx_trusted → load_postgres → dbt_run → dbt_test
    → dbt_docs_generate → export_delivery → gx_delivery → openmetadata_catalog
```

O disparo é manual por opção de projeto (`schedule=None`): as fontes são
arquivos estáticos em `data/raw_free`, então não há dado novo chegando que
justifique agendamento.

---

## 9. Conferir o resultado

**O sinal mais importante** é que os arquivos mudaram no repositório. Se as
tasks passaram mas os arquivos não mudaram, o `HOST_PROJECT_DIR` está apontando
para o vazio:

```powershell
Get-ChildItem data\trusted_parquet, data\delivered_gold | Select-Object Name, LastWriteTime
```

```bash
ls -l data/trusted_parquet data/delivered_gold
```

O banco:

```powershell
docker compose exec postgres-db psql -U postgres -d pipeline_db -c "\dt trusted_atv4.*"
docker compose exec postgres-db psql -U postgres -d pipeline_db -c "select count(*) from delivery_atv4.delivery_reclamacoes_satisfacao;"
```

```bash
docker compose exec postgres-db psql -U postgres -d pipeline_db -c '\dt trusted_atv4.*'
docker compose exec postgres-db psql -U postgres -d pipeline_db -c 'select count(*) from delivery_atv4.delivery_reclamacoes_satisfacao;'
```

Esperado: 4 tabelas em `trusted_atv4` e **918 linhas** no mart.

E as interfaces:

- **http://localhost:8182** — Data Docs do Great Expectations, com as 5 suites e
  73 expectations. O contexto é persistente, então o histórico se acumula a cada
  execução.
- **http://localhost:8181** — documentação do dbt, com o lineage dos modelos.
- **http://localhost:8585** — o serviço `atv4_postgres`, com os schemas
  `trusted_atv4` e `delivery_atv4` catalogados.

---

## 10. Provar que o gate de qualidade funciona

Rodar o pipeline com sucesso mostra que a orquestração funciona. O que prova que
ela **protege** alguma coisa é vê-la barrar dado ruim.

Em `quality/validate_trusted_bancos.py`, troque temporariamente o domínio de
`segmento`:

```python
value_set=["S1"],   # era ["S1", "S2", "S3", "S4", "S5"]
```

Reconstrua a imagem e dispare de novo:

```powershell
docker compose --profile build build app
docker compose exec --user airflow airflow-scheduler airflow dags trigger atv4_medallion_end_to_end
```

```bash
docker compose --profile build build app
docker compose exec --user airflow airflow-scheduler airflow dags trigger atv4_medallion_end_to_end
```

O esperado é `gx_trusted` **falhar** e todas as tasks seguintes ficarem em
`upstream_failed`. Ou seja: dado reprovado não chega ao Postgres nem à camada
Delivery. Depois é só restaurar a linha e reconstruir.

---

## 11. Parar e recomeçar

```powershell
docker compose stop            # pausa, preservando os dados
docker compose up -d           # retoma
docker compose down            # remove os containers, preserva os volumes
docker compose down -v         # apaga tudo, inclusive o banco — recomeço do zero
```

```bash
docker compose stop            # pausa, preservando os dados
docker compose up -d           # retoma
docker compose down            # remove os containers, preserva os volumes
docker compose down -v         # apaga tudo, inclusive o banco — recomeço do zero
```

Depois de um `down -v`, volte ao passo 4 (e o token do passo 7 precisará ser
gerado de novo, porque o `openmetadata_db` foi recriado).

---

## Quando algo falha

### `ModuleNotFoundError: No module named 'airflow'` ao usar `docker compose exec`

Falta o `--user airflow`. O scheduler roda como `root` para conseguir abrir o
socket do Docker, mas o Airflow está instalado em
`/home/airflow/.local/lib/python3.13/site-packages` — o *user-site* do usuário
`airflow`, que não entra no `sys.path` do root.

```powershell
docker compose exec --user airflow airflow-scheduler airflow dags list
```

```bash
docker compose exec --user airflow airflow-scheduler airflow dags list
```

Isso afeta apenas comandos manuais. O scheduler em si funciona normalmente,
porque o entrypoint da imagem prepara o ambiente do processo principal.

### O Elasticsearch não fica `healthy`

Quase sempre é memória. Suba o Docker Desktop para 10 GB, ou reduza o heap em
`docker-compose.yml`:

```yaml
- ES_JAVA_OPTS=-Xms384m -Xmx384m
```

### As tasks ficam presas em `queued` e nunca começam

Sintoma clássico de `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` errada — sem ela
o scheduler procura a Execution API em `localhost:8080` dentro do próprio
container. O valor correto já está no compose
(`http://airflow-apiserver:8080/execution/`, com a porta **interna** 8080, não a
8081 publicada no host). Confirme que o apiserver está `healthy`.

### `PermissionError` em `/var/run/docker.sock`

O scheduler precisa abrir o socket para criar os containers das tasks, e por
isso roda como `root` (`user: "0:0"` no compose). Se você tirar essa linha, o
erro volta: no Docker Desktop o socket aparece dentro do container como
`root:root 0660`, inacessível para o uid 50000 do Airflow.

Para inspecionar o socket de dentro do container:

```powershell
docker compose exec airflow-scheduler ls -l /var/run/docker.sock
```

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler ls -l //var/run/docker.sock
```

Vale saber do trade-off: root com acesso ao socket equivale a root no host. É
aceitável neste ambiente local; em qualquer outro, o caminho seria um
`docker-socket-proxy` com whitelist de endpoints.

### As tasks passam, mas nenhum arquivo muda no repositório

`HOST_PROJECT_DIR` errada. Veja o passo 2. Para confirmar o que o container
enxergou:

```powershell
docker compose exec --user airflow airflow-scheduler printenv HOST_PROJECT_DIR
```

```bash
docker compose exec --user airflow airflow-scheduler printenv HOST_PROJECT_DIR
```

### `openmetadata_catalog` falha com 401 ou reclamando do token

`OM_JWT_TOKEN` vazio, expirado ou revogado. Refaça o passo 7 — o token pode ser
revogado e regerado na mesma tela.

### O container filho não encontra `postgres-db`

A DAG usa `network_mode="atv4_private"`, que é o nome fixo definido no compose.
Confirme que a rede existe:

```powershell
docker network ls | Select-String atv4
```

```bash
docker network ls | grep atv4
```

### Um serviço aparece `unhealthy` mas responde normalmente

Vale conferir se o healthcheck usa uma ferramenta que existe na imagem. Foi o
caso do OpenMetadata: o healthcheck original usava `curl`, e a imagem só tem
`wget` — o servidor respondia HTTP 200 e mesmo assim aparecia como `unhealthy`.
Para testar por fora:

```powershell
curl.exe -s -o NUL -w "%{http_code}`n" http://localhost:8586/healthcheck
```

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8586/healthcheck
```

### As tabelas ou os databases não existem

O `init.sql` só roda quando o volume do Postgres está vazio. Se você reaproveitou
um volume antigo, os schemas `trusted_atv4` / `delivery_atv4` e os databases
`airflow_db` / `openmetadata_db` não foram criados. Solução: `docker compose down -v`
e recomeçar do passo 4.

---

## Sem orquestrador

O pipeline continua executável à mão, uma etapa por vez — útil para depurar uma
etapa isolada sem passar pelo Airflow:

```powershell
docker compose --profile build run --rm app                                   # transform + gate + load
docker compose --profile build run --rm app uv run python -m src.cli export
docker compose --profile build run --rm elt_transformation dbt run
docker compose --profile build run --rm elt_transformation dbt test
```

```bash
docker compose --profile build run --rm app                                   # transform + gate + load
docker compose --profile build run --rm app uv run python -m src.cli export
docker compose --profile build run --rm elt_transformation dbt run
docker compose --profile build run --rm elt_transformation dbt test
```

As etapas disponíveis em `src/cli.py` são `transform`, `quality-trusted`,
`load`, `export`, `quality-delivery` e `catalog` — as mesmas que o Airflow chama.
