# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Coleta e camada Bronze
# MAGIC
# MAGIC **Etapa do MVP:** 4.2 (Coleta)
# MAGIC
# MAGIC Faço a ingestão **via API**, não por upload manual. O fluxo é:
# MAGIC
# MAGIC ```
# MAGIC API CKAN do ONS  ──►  Volume (/Volumes/.../raw_ons/<dataset>/arquivo.csv)  ──►  Tabela Delta Bronze
# MAGIC   (descoberta)          (landing zone, arquivo preservado)                     (todas colunas STRING)
# MAGIC ```
# MAGIC
# MAGIC ### Por que descobri as URLs pela API em vez de fixá-las no código
# MAGIC
# MAGIC O ONS publica os arquivos no S3 sob caminhos que **não seguem** o nome do pacote CKAN.
# MAGIC O pacote `restricao_coff_eolica_usi`, por exemplo, publica em `restricao_coff_eolica_tm/`.
# MAGIC URLs montadas à mão quebram silenciosamente. Consultando `package_show` a cada execução,
# MAGIC o pipeline também captura automaticamente os meses novos que o ONS publicar.
# MAGIC
# MAGIC ### Regra da camada Bronze
# MAGIC
# MAGIC Não aplico nenhuma regra de negócio aqui. Todas as colunas entram como `STRING` (`inferSchema=False`)
# MAGIC justamente para **não perder informação**: se um campo numérico vier com lixo, o lixo fica
# MAGIC registrado no Bronze e eu o trato — de forma explícita e auditável — no Silver.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import os
import time
import urllib.request
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Teste de conectividade — rode isto primeiro
# MAGIC
# MAGIC A Databricks **Free Edition restringe o acesso à internet a um conjunto limitado de
# MAGIC domínios confiáveis**, e a lista não é publicada. Os domínios do ONS provavelmente não
# MAGIC estão nela por padrão.
# MAGIC
# MAGIC **Como liberar:** a própria Databricks concede *outbound internet access* ao verificar a
# MAGIC conta com o LinkedIn (`Settings` → o banner de verificação no workspace). É gratuito e é o
# MAGIC caminho oficial.
# MAGIC
# MAGIC A célula abaixo testa os dois domínios necessários em poucos segundos, para você descobrir
# MAGIC isso agora e não depois de meia hora de execução. Se falhar, veja a seção 2.1 (plano B).

# COMMAND ----------

def testar_conectividade() -> bool:
    """Verifica se os domínios do ONS estão acessíveis a partir deste workspace."""
    alvos = [
        ("API CKAN do ONS", "https://dados.ons.org.br/api/3/action/package_list"),
        ("Arquivos (S3 do ONS)",
         "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/ear_subsistema_di/"
         "EAR_DIARIO_SUBSISTEMA_2025.csv"),
    ]
    tudo_ok = True
    for rotulo, url in alvos:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MVP-EngDados-PUCRJ/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp.read(2048)
            print(f"  [OK]    {rotulo}")
        except Exception as e:
            tudo_ok = False
            print(f"  [FALHA] {rotulo}\n          {type(e).__name__}: {str(e)[:160]}")
    return tudo_ok


if testar_conectividade():
    print(
        "\nConectividade confirmada. Pode seguir com o notebook normalmente."
    )
else:
    print(
        "\n" + "=" * 78 + "\n"
        "SEM ACESSO AOS DOMÍNIOS DO ONS A PARTIR DESTE WORKSPACE.\n\n"
        "Causa provável: a Free Edition restringe o acesso à internet a domínios confiáveis.\n\n"
        "Solução 1 (recomendada) — verifique a conta com o LinkedIn nas configurações do\n"
        "  workspace. A Databricks concede 'outbound internet access' na verificação.\n"
        "  Depois, reexecute esta célula.\n\n"
        "Solução 2 (plano B) — baixe os arquivos na sua máquina e suba para o Volume pela UI.\n"
        "  Veja a seção 2.1 deste notebook: as tabelas Bronze são criadas a partir dos\n"
        "  arquivos que estiverem no Volume, não importa como chegaram lá.\n"
        + "=" * 78
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Descoberta dos arquivos na API

# COMMAND ----------

plano = {}
for chave, meta in DATASETS.items():
    recursos = listar_recursos_csv(meta["package"])
    selecionados = filtrar_por_periodo(recursos, meta["periodicidade"])
    plano[chave] = selecionados
    print(
        f"{chave:<32} {len(recursos):>4} CSV publicados  ->  "
        f"{len(selecionados):>3} selecionados para a janela do MVP"
    )

total = sum(len(v) for v in plano.values())
print(f"\nTotal de arquivos a coletar: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download para o Volume
# MAGIC
# MAGIC Fiz o download **idempotente**: arquivos já presentes no Volume com tamanho > 0 são pulados.
# MAGIC Assim posso reexecutar o notebook após uma falha de rede sem baixar tudo de novo — os
# MAGIC arquivos de constrained-off eólico têm ~23 MB cada.
# MAGIC
# MAGIC Cada download alimenta um **manifesto de ingestão**, que é a evidência de linhagem exigida
# MAGIC na etapa 4.3: para cada arquivo, de qual URL veio, quando foi coletado e quantos bytes tem.

# COMMAND ----------

manifesto = []

for chave, recursos in plano.items():
    destino_dir = f"{VOLUME_PATH}/{chave}"
    os.makedirs(destino_dir, exist_ok=True)

    for r in recursos:
        nome_arquivo = r["url"].rsplit("/", 1)[-1]
        destino = f"{destino_dir}/{nome_arquivo}"

        if os.path.exists(destino) and os.path.getsize(destino) > 0:
            status, tamanho = "cache", os.path.getsize(destino)
        else:
            inicio = time.time()
            req = urllib.request.Request(
                r["url"], headers={"User-Agent": "MVP-EngDados-PUCRJ/1.0"}
            )
            with urllib.request.urlopen(req, timeout=300) as resp, open(destino, "wb") as f:
                while True:
                    bloco = resp.read(1 << 20)  # 1 MiB por vez, evita estourar memória do driver
                    if not bloco:
                        break
                    f.write(bloco)
            tamanho = os.path.getsize(destino)
            status = f"baixado em {time.time() - inicio:.1f}s"

        manifesto.append(
            {
                "dataset": chave,
                "arquivo": nome_arquivo,
                "url_origem": r["url"],
                "caminho_volume": destino,
                "bytes": tamanho,
                "status": status,
                "ingest_ts": datetime.now().isoformat(timespec="seconds"),
            }
        )
        print(f"  {chave:<30} {nome_arquivo:<45} {tamanho/1048576:>7.1f} MB  [{status}]")

print(f"\nTotal aterrissado: {sum(m['bytes'] for m in manifesto)/1048576:.1f} MB "
      f"em {len(manifesto)} arquivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Plano B — se o acesso à internet estiver bloqueado
# MAGIC
# MAGIC A camada Bronze é criada a partir dos **arquivos que estiverem no Volume**, e a leitura não
# MAGIC sabe nem se importa como eles chegaram lá. Isso significa que o upload manual é um caminho
# MAGIC alternativo válido, sem alterar nenhuma linha das etapas seguintes.
# MAGIC
# MAGIC **Como fazer:**
# MAGIC
# MAGIC 1. Na sua máquina, baixe os CSVs do portal do ONS (https://dados.ons.org.br), respeitando
# MAGIC    a janela configurada em `_config`:
# MAGIC    - `balanco-energia-subsistema` — anos 2021 a 2026
# MAGIC    - `ear-diario-por-subsistema` — anos 2021 a 2026
# MAGIC    - `restricao_coff_eolica_usi` — meses de 2025-01 a 2026-08
# MAGIC    - `restricao_coff_fotovoltaica` — meses de 2025-01 a 2026-08
# MAGIC    - `capacidade-geracao` — arquivo único
# MAGIC 2. No Catalog Explorer, abra o Volume `mvp_ons.bronze.raw_ons`.
# MAGIC 3. Crie **uma subpasta por dataset**, com exatamente estes nomes — a leitura depende deles:
# MAGIC    `balanco_energia_subsistema`, `ear_diario_subsistema`, `restricao_coff_eolica`,
# MAGIC    `restricao_coff_fotovoltaica`, `capacidade_geracao`
# MAGIC 4. Faça o upload dos CSVs em cada subpasta (botão **Upload to this volume**).
# MAGIC 5. **Pule as células 1, 2 e 3** deste notebook e execute direto a partir da seção 4.
# MAGIC
# MAGIC Se optar por esse caminho, o manifesto de linhagem (`bronze._manifesto_ingestao`) não será
# MAGIC gerado, já que ele registra a URL de origem de cada download. Registre no README que a
# MAGIC coleta foi manual — é uma limitação da plataforma, não um defeito do pipeline.
# MAGIC
# MAGIC > Para reduzir o volume, é legítimo estreitar a janela em `_config` (por exemplo,
# MAGIC > `COFF_INICIO = "2025-07"`). Documente a janela efetivamente usada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Persistência do manifesto de ingestão

# COMMAND ----------

df_manifesto = spark.createDataFrame(manifesto).withColumn(
    "_ingest_ts", F.current_timestamp()
)
df_manifesto.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    BRONZE("_manifesto_ingestao")
)

spark.sql(
    f"COMMENT ON TABLE {BRONZE('_manifesto_ingestao')} IS "
    "'Manifesto de linhagem da coleta: para cada arquivo aterrissado no Volume, registra a URL "
    "de origem na API CKAN do ONS, o caminho no Volume, o tamanho e o instante da ingestão.'"
)
display(spark.table(BRONZE("_manifesto_ingestao")).groupBy("dataset").agg(
    F.count("*").alias("arquivos"),
    F.round(F.sum("bytes") / 1048576, 1).alias("mb_total"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criação das tabelas Bronze
# MAGIC
# MAGIC Leitura dos CSVs do Volume com `sep=';'`, `encoding='UTF-8'`, `multiLine=true` e **todas as colunas como STRING**.
# MAGIC O `multiLine` é necessário porque o ONS publica campos de texto com quebra de linha entre aspas;
# MAGIC sem ele, esses registros seriam partidos em fragmentos e perdidos na Silver.
# MAGIC Acrescento três colunas de controle, que é a única alteração que me permito no Bronze:
# MAGIC
# MAGIC | Coluna | Significado |
# MAGIC |---|---|
# MAGIC | `_ingest_ts` | Instante em que a linha foi carregada na tabela |
# MAGIC | `_source_file` | Caminho completo do arquivo de origem no Volume |
# MAGIC | `_source_dataset` | Chave do dataset no portal do ONS |

# COMMAND ----------

def carregar_bronze(chave: str, descricao: str) -> int:
    """Lê todos os CSVs de um dataset no Volume e materializa a tabela Bronze."""
    caminho = f"{VOLUME_PATH}/{chave}/*.csv"

    df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")  # Bronze preserva tudo como texto
        # O ONS publica campos de texto com quebra de linha entre aspas (dsc_restricao,
        # nom_pontoconexao). Sem multiLine, o Spark parte cada um desses registros em
        # fragmentos inválidos — com multiLine, o registro é lido inteiro, como publicado.
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(caminho)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_dataset", F.lit(chave))
    )

    tabela = BRONZE(chave)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc_sql(descricao)}'")

    n = spark.table(tabela).count()
    print(f"[OK] {tabela:<45} {n:>12,} linhas")
    return n

# COMMAND ----------

descricoes_bronze = {
    "balanco_energia_subsistema": (
        "BRONZE — Balanço de energia por subsistema em base horária, cru. Fonte: ONS, dataset "
        "balanco-energia-subsistema (CC-BY). Contém geração por fonte, carga e intercâmbio. "
        "ATENÇÃO: inclui linhas com id_subsistema=SIN, que são o AGREGADO nacional e não um "
        "subsistema — somar tudo sem filtrar duplica a energia."
    ),
    "ear_diario_subsistema": (
        "BRONZE — Energia Armazenada Verificada (EAR) diária por subsistema, cru. Fonte: ONS, "
        "dataset ear-diario-por-subsistema (CC-BY). Proxy do nível dos reservatórios."
    ),
    "restricao_coff_eolica": (
        "BRONZE — Restrição de operação por constrained-off de usinas EÓLICAS, em base "
        "semi-horária e por usina. Fonte: ONS, dataset restricao_coff_eolica_usi (CC-BY)."
    ),
    "restricao_coff_fotovoltaica": (
        "BRONZE — Restrição de operação por constrained-off de usinas FOTOVOLTAICAS, em base "
        "semi-horária e por usina. Fonte: ONS, dataset restricao_coff_fotovoltaica (CC-BY)."
    ),
    "capacidade_geracao": (
        "BRONZE — Capacidade instalada de geração por unidade geradora, cru. Fonte: ONS, "
        "dataset capacidade-geracao (CC-BY). Usado para enriquecer a dimensão de usina."
    ),
}

linhas = {c: carregar_bronze(c, d) for c, d in descricoes_bronze.items()}
print(f"\nTotal na camada Bronze: {sum(linhas.values()):,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência — o Bronze é mesmo uma cópia fiel?
# MAGIC
# MAGIC Amostra de uma tabela para evidenciar que os dados estão como o ONS publicou, inclusive
# MAGIC com as imperfeições (campos vazios, sentinela `-` em `ceg`, espaços à direita). Essas
# MAGIC imperfeições eu trato no notebook seguinte, não aqui.

# COMMAND ----------

display(spark.table(BRONZE("restricao_coff_eolica")).limit(10))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {BRONZE()}"))

# COMMAND ----------

print("Coleta concluída. Próximo passo: notebook `02_silver_limpeza`.")
