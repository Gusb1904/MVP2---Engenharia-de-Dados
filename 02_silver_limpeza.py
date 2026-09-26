# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Camada Silver: limpeza e padronização
# MAGIC
# MAGIC **Etapa do MVP:** 4.4 (Carga/ETL — transformação) e 4.5 (tratamento de qualidade)
# MAGIC
# MAGIC Aqui o dado deixa de ser texto cru e passa a ser confiável. Cada transformação abaixo
# MAGIC responde a um problema **concreto que observei** nos dados do ONS, não a uma limpeza
# MAGIC genérica de manual.
# MAGIC
# MAGIC | # | Problema observado no Bronze | Tratamento no Silver | Impacto |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Tudo é `STRING` (Bronze por definição) | `CAST` para `DOUBLE`, `TIMESTAMP`, `DATE` | Viabiliza agregação e ordenação temporal |
# MAGIC | 2 | `nom_subsistema` vem com espaços à direita em `capacidade_geracao` (`'NORDESTE       '`) | `TRIM` em todas as colunas de texto | Sem isso, `'NORDESTE'` e `'NORDESTE '` viram duas chaves distintas no JOIN |
# MAGIC | 3 | Campos vazios `''` em vez de `NULL` (59,9% na eólica e 72,5% na solar em `cod_razaorestricao`) | String vazia → `NULL` | `COUNT` e média passam a ignorar ausência corretamente |
# MAGIC | 4 | `'-'` no campo `ceg` — o dicionário do ONS usa `-` para **conjuntos de usinas**, que não têm CEG | `'-'` → `NULL` | A ausência de CEG vira ausência real, sem se passar por um código |
# MAGIC | 5 | Linhas `id_subsistema = 'SIN'` misturadas com subsistemas reais no balanço | Coluna `eh_agregado` separa os dois | **Crítico**: somar sem filtrar duplica toda a energia do país |
# MAGIC | 6 | Caixa inconsistente de nomes entre datasets (`'Nordeste'` vs `'NORDESTE'`) | Padronização por `id_subsistema` (a chave estável) | JOINs entre datasets passam a fechar |
# MAGIC | 7 | Possíveis linhas repetidas em recargas de arquivo | `dropDuplicates` na chave de negócio | Garante a granularidade declarada |
# MAGIC | 8 | Duas fontes separadas (eólica / fotovoltaica) com schema idêntico | `UNION` com coluna `cod_fonte` | Uma tabela única de restrição, analisável em conjunto |

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Funções de limpeza reutilizáveis
# MAGIC
# MAGIC Defino uma vez e aplico em todos os datasets, o que evita que a regra de limpeza divirja
# MAGIC entre tabelas — que é como duas tabelas da mesma camada acabam com padrões diferentes.

# COMMAND ----------

# Valores que o ONS usa para representar ausência de informação.
SENTINELAS = ["", "-", "--", "N/A", "NA", "null", "NULL"]


def texto_limpo(coluna: str):
    """TRIM + conversão de sentinelas para NULL real."""
    limpo = F.trim(F.col(coluna))
    return F.when(limpo.isin(SENTINELAS), F.lit(None)).otherwise(limpo)


def decimal_limpo(coluna: str):
    """Converte texto numérico para DOUBLE, tolerando vazio e separador de milhar.

    Os arquivos do ONS usam ponto como separador decimal, mas a limpeza remove
    eventual separador de milhar antes do CAST para não gerar NULL silencioso.
    """
    limpo = F.trim(F.col(coluna))
    limpo = F.when(limpo.isin(SENTINELAS), F.lit(None)).otherwise(limpo)
    return F.regexp_replace(limpo, ",", "").cast("double")


def salvar_silver(df: DataFrame, nome: str, descricao: str, chave: list) -> None:
    """Deduplica pela chave de negócio, persiste como Delta e documenta a tabela."""
    antes = df.count()
    df = df.dropDuplicates(chave)
    depois = df.count()

    tabela = SILVER(nome)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc_sql(descricao)}'")

    removidas = antes - depois
    print(
        f"[OK] {tabela:<45} {depois:>12,} linhas"
        + (f"  ({removidas:,} duplicatas removidas)" if removidas else "  (sem duplicatas)")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.balanco_subsistema_horario`
# MAGIC
# MAGIC **Granularidade:** um registro por subsistema por hora.
# MAGIC
# MAGIC A transformação mais importante desta tabela é a coluna `eh_agregado`. O arquivo do ONS
# MAGIC mistura, nas mesmas linhas, os quatro subsistemas reais **e** o total `SIN`. Quem fizer
# MAGIC `SUM(val_carga)` sem perceber isso obtém exatamente o dobro da carga do país. Em vez de
# MAGIC apagar as linhas `SIN` (que são úteis para conferência), eu as marco.
# MAGIC
# MAGIC Também derivo aqui as métricas que sustentam as perguntas P2 e P7:
# MAGIC `ger_intermitente` (eólica + solar) e `carga_liquida` (carga − intermitente).

# COMMAND ----------

df_balanco = (
    spark.table(BRONZE("balanco_energia_subsistema"))
    .select(
        texto_limpo("id_subsistema").alias("id_subsistema"),
        texto_limpo("nom_subsistema").alias("nom_subsistema_origem"),
        F.to_timestamp(F.trim(F.col("din_instante")), "yyyy-MM-dd HH:mm:ss").alias("din_instante"),
        decimal_limpo("val_gerhidraulica").alias("ger_hidraulica_mwmed"),
        decimal_limpo("val_gertermica").alias("ger_termica_mwmed"),
        decimal_limpo("val_gereolica").alias("ger_eolica_mwmed"),
        decimal_limpo("val_gersolar").alias("ger_solar_mwmed"),
        decimal_limpo("val_carga").alias("carga_mwmed"),
        decimal_limpo("val_intercambio").alias("intercambio_mwmed"),
        F.col("_source_file"),
        F.col("_ingest_ts"),
    )
    # Problema 5: separa o agregado nacional dos subsistemas reais.
    .withColumn("eh_agregado", F.col("id_subsistema") == F.lit("SIN"))
    # Métricas derivadas — base das perguntas P1, P2 e P7.
    .withColumn(
        "ger_total_mwmed",
        F.coalesce(F.col("ger_hidraulica_mwmed"), F.lit(0.0))
        + F.coalesce(F.col("ger_termica_mwmed"), F.lit(0.0))
        + F.coalesce(F.col("ger_eolica_mwmed"), F.lit(0.0))
        + F.coalesce(F.col("ger_solar_mwmed"), F.lit(0.0)),
    )
    .withColumn(
        "ger_intermitente_mwmed",
        F.coalesce(F.col("ger_eolica_mwmed"), F.lit(0.0))
        + F.coalesce(F.col("ger_solar_mwmed"), F.lit(0.0)),
    )
    .withColumn("carga_liquida_mwmed", F.col("carga_mwmed") - F.col("ger_intermitente_mwmed"))
    .withColumn(
        "pct_intermitente_sobre_carga",
        F.when(
            F.col("carga_mwmed") > 0,
            F.round(100 * F.col("ger_intermitente_mwmed") / F.col("carga_mwmed"), 4),
        ),
    )
    # Registros sem instante válido não têm uso analítico nem como corrigir.
    .filter(F.col("din_instante").isNotNull())
)

salvar_silver(
    df_balanco,
    "balanco_subsistema_horario",
    "SILVER — Balanço de energia por subsistema em base horária, limpo e tipado. "
    "Granularidade: 1 linha por subsistema por hora. Inclui métricas derivadas "
    "(geração total, intermitente, carga líquida e participação intermitente sobre a carga). "
    "A coluna eh_agregado=true marca a linha SIN (total nacional): filtre eh_agregado=false "
    "para somar por subsistema sem dupla contagem. Linhagem: bronze.balanco_energia_subsistema.",
    chave=["id_subsistema", "din_instante"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.ear_subsistema_diario`
# MAGIC
# MAGIC **Granularidade:** um registro por subsistema por dia.
# MAGIC
# MAGIC EAR = Energia Armazenada Verificada, o "nível do reservatório" expresso em energia.
# MAGIC É o que permite responder a P6: quando os reservatórios estão cheios, sobra menos espaço
# MAGIC para absorver geração renovável, e o corte tende a subir.

# COMMAND ----------

df_ear = (
    spark.table(BRONZE("ear_diario_subsistema"))
    .select(
        texto_limpo("id_subsistema").alias("id_subsistema"),
        F.to_date(F.trim(F.col("ear_data")), "yyyy-MM-dd").alias("dat_referencia"),
        decimal_limpo("ear_max_subsistema").alias("ear_maxima_mwmes"),
        decimal_limpo("ear_verif_subsistema_mwmes").alias("ear_verificada_mwmes"),
        decimal_limpo("ear_verif_subsistema_percentual").alias("ear_percentual"),
        F.col("_source_file"),
        F.col("_ingest_ts"),
    )
    # O arquivo de EAR não traz linha SIN hoje (só o balanço traz). A marcação fica como
    # proteção caso o ONS passe a publicá-la, sem custo quando ela não existe.
    .withColumn("eh_agregado", F.col("id_subsistema") == F.lit("SIN"))
    .filter(F.col("dat_referencia").isNotNull())
)

salvar_silver(
    df_ear,
    "ear_subsistema_diario",
    "SILVER — Energia Armazenada Verificada (EAR) diária por subsistema, limpa e tipada. "
    "Granularidade: 1 linha por subsistema por dia. ear_percentual é o nível de armazenamento "
    "em % da capacidade máxima (0 a 100). Linhagem: bronze.ear_diario_subsistema.",
    chave=["id_subsistema", "dat_referencia"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.restricao_coff_usina` — a tabela central do MVP
# MAGIC
# MAGIC **Granularidade:** um registro por usina a cada 30 minutos.
# MAGIC
# MAGIC ### União das duas fontes
# MAGIC Eólica e fotovoltaica vêm em datasets separados com schema idêntico. Eu as unifico com uma
# MAGIC coluna `cod_fonte` que preserva a origem — sem isso, seria impossível comparar o
# MAGIC comportamento das duas tecnologias (P3, P5).
# MAGIC
# MAGIC ### Cálculo da energia cortada — e a premissa por trás dele
# MAGIC O ONS publica cinco medidas por registro. Duas interessam:
# MAGIC
# MAGIC - `val_geracaoreferencia`: quanto a usina **teria gerado** com o recurso disponível
# MAGIC - `val_geracao`: quanto ela **efetivamente gerou**
# MAGIC
# MAGIC A diferença é a energia cortada. Existe também `val_geracaoreferenciafinal`, mas o
# MAGIC dicionário do ONS esclarece que ela **só é calculada nos períodos com restrição do tipo REL**
# MAGIC (é o valor que o ONS envia à CCEE para ressarcimento). Por isso ela não serve de base para
# MAGIC os demais tipos de restrição. A premissa que adotei, portanto, é:
# MAGIC
# MAGIC ```
# MAGIC corte_mwmed = MAX(val_geracaoreferencia − val_geracao, 0)   -- apenas quando há razão de restrição
# MAGIC ```
# MAGIC
# MAGIC O `MAX(..., 0)` descarta diferenças negativas, que representam geração acima da referência
# MAGIC (erro de previsão do recurso, não corte). A restrição a registros com `cod_razaorestricao`
# MAGIC preenchido evita contar como corte qualquer desvio normal de previsão.
# MAGIC
# MAGIC Esta é a mesma regra da **Geração Não Realizada Apurada (GNRa)** descrita pelo ONS. Na versão
# MAGIC 1.5 do dicionário (08/09/2026) o ONS passou a publicar a GNRa oficial na coluna
# MAGIC `val_geracaonaorealizadaapurada`. Mantenho o meu cálculo e trago a GNRa oficial ao lado
# MAGIC (`gnra_oficial_mwh`), para validar um contra o outro no notebook 04.
# MAGIC
# MAGIC ### Conversão para energia
# MAGIC Os valores estão em MWmed (potência média no intervalo). Como o intervalo é de 30 minutos,
# MAGIC a energia em MWh é `MWmed × 0,5`.

# COMMAND ----------

def preparar_restricao(tabela_bronze: str, cod_fonte: str, nome_fonte: str) -> DataFrame:
    """Normaliza um dataset de constrained-off para o schema comum do Silver."""
    return (
        spark.table(tabela_bronze)
        .select(
            texto_limpo("id_subsistema").alias("id_subsistema"),
            texto_limpo("id_estado").alias("id_estado"),
            texto_limpo("nom_estado").alias("nom_estado"),
            texto_limpo("nom_usina").alias("nom_usina"),
            texto_limpo("id_ons").alias("id_ons"),
            texto_limpo("ceg").alias("ceg"),  # Problema 4: sentinela '-' vira NULL
            F.to_timestamp(F.trim(F.col("din_instante")), "yyyy-MM-dd HH:mm:ss").alias("din_instante"),
            decimal_limpo("val_geracao").alias("geracao_mwmed"),
            decimal_limpo("val_geracaolimitada").alias("geracao_limitada_mwmed"),
            decimal_limpo("val_disponibilidade").alias("disponibilidade_mwmed"),
            decimal_limpo("val_geracaoreferencia").alias("geracao_referencia_mwmed"),
            decimal_limpo("val_geracaoreferenciafinal").alias("geracao_referencia_final_mwmed"),
            texto_limpo("cod_razaorestricao").alias("cod_razao_restricao"),
            texto_limpo("cod_origemrestricao").alias("cod_origem_restricao"),
            texto_limpo("dsc_restricao").alias("dsc_restricao"),
            # Colunas incluídas pelo ONS na versão 1.5 do dicionário (08/09/2026)
            texto_limpo("id_pontoconexao").alias("id_ponto_conexao"),
            texto_limpo("nom_pontoconexao").alias("nom_ponto_conexao"),
            texto_limpo("nom_agenteoperador").alias("nom_agente_operador"),
            decimal_limpo("val_geracaonaorealizadaapurada").alias("gnra_oficial_mwmed"),
            decimal_limpo("num_minutos_rel").cast("int").alias("num_minutos_rel"),
            decimal_limpo("num_minutos_cnf").cast("int").alias("num_minutos_cnf"),
            decimal_limpo("num_minutos_ene").cast("int").alias("num_minutos_ene"),
            decimal_limpo("num_minutos_restricao").cast("int").alias("num_minutos_restricao"),
            F.col("_source_file"),
            F.col("_ingest_ts"),
        )
        .withColumn("cod_fonte", F.lit(cod_fonte))
        .withColumn("nom_fonte", F.lit(nome_fonte))
    )


df_restricao = preparar_restricao(
    BRONZE("restricao_coff_eolica"), "EOL", "Eólica"
).unionByName(
    preparar_restricao(BRONZE("restricao_coff_fotovoltaica"), "SOL", "Solar")
)

# COMMAND ----------

df_restricao = (
    df_restricao
    .filter(F.col("din_instante").isNotNull())
    .withColumn("houve_restricao", F.col("cod_razao_restricao").isNotNull())
    # Energia cortada: só conta quando o ONS declarou uma razão de restrição.
    .withColumn(
        "corte_mwmed",
        F.when(
            F.col("cod_razao_restricao").isNotNull(),
            F.greatest(
                F.col("geracao_referencia_mwmed") - F.col("geracao_mwmed"), F.lit(0.0)
            ),
        ).otherwise(F.lit(0.0)),
    )
    # Intervalo de 30 min -> energia em MWh é a potência média x 0,5 h.
    .withColumn("corte_mwh", F.round(F.col("corte_mwmed") * F.lit(0.5), 6))
    .withColumn("geracao_mwh", F.round(F.col("geracao_mwmed") * F.lit(0.5), 6))
    .withColumn(
        "geracao_referencia_mwh",
        F.round(F.col("geracao_referencia_mwmed") * F.lit(0.5), 6),
    )
    .withColumn("gnra_oficial_mwh", F.round(F.col("gnra_oficial_mwmed") * F.lit(0.5), 6))
    # Tradução dos códigos do ONS para rótulos legíveis, conforme o dicionário oficial v1.5.
    .withColumn(
        "dsc_razao_restricao",
        F.when(F.col("cod_razao_restricao") == "REL", "Indisponibilidade externa (elétrica)")
        .when(F.col("cod_razao_restricao") == "CNF", "Requisitos de confiabilidade")
        .when(F.col("cod_razao_restricao") == "ENE", "Razão energética")
        .when(F.col("cod_razao_restricao") == "PAR", "Parecer de acesso")
        .otherwise(F.lit(None)),
    )
    .withColumn(
        "dsc_origem_restricao",
        F.when(F.col("cod_origem_restricao") == "SIS", "Sistêmica")
        .when(F.col("cod_origem_restricao") == "LOC", "Local")
        .otherwise(F.lit(None)),
    )
)

salvar_silver(
    df_restricao,
    "restricao_coff_usina",
    "SILVER — Restrição de operação por constrained-off de usinas eólicas e fotovoltaicas, "
    "unificadas. Granularidade: 1 linha por usina a cada 30 minutos. corte_mwh é a energia "
    "cortada, calculada como MAX(geracao_referencia - geracao, 0) x 0,5h, apenas quando há "
    "razão de restrição declarada pelo ONS. Linhagem: união de bronze.restricao_coff_eolica "
    "e bronze.restricao_coff_fotovoltaica.",
    chave=["id_ons", "din_instante", "cod_fonte"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `silver.capacidade_instalada_ug`
# MAGIC
# MAGIC **Granularidade:** um registro por unidade geradora.
# MAGIC
# MAGIC Esta é a tabela onde o problema 2 aparece de forma mais extensa: **todas** as linhas trazem
# MAGIC `nom_subsistema`, `cod_equipamento` e `num_unidadegeradora` completados com espaços à direita
# MAGIC (largura fixa). Aplico `TRIM` em todas as colunas de texto. O JOIN desta tabela com a
# MAGIC restrição, porém, é feito pelo código CEG e não pelo nome — o notebook 03 explica por quê.

# COMMAND ----------

df_capacidade = (
    spark.table(BRONZE("capacidade_geracao"))
    .select(
        texto_limpo("id_subsistema").alias("id_subsistema"),
        texto_limpo("id_estado").alias("id_estado"),
        texto_limpo("nom_estado").alias("nom_estado"),
        texto_limpo("nom_tipousina").alias("nom_tipo_usina"),
        texto_limpo("nom_usina").alias("nom_usina"),
        texto_limpo("ceg").alias("ceg"),
        texto_limpo("nom_combustivel").alias("nom_combustivel"),
        texto_limpo("nom_agenteproprietario").alias("nom_agente_proprietario"),
        texto_limpo("cod_equipamento").alias("cod_equipamento"),
        F.to_date(F.trim(F.col("dat_entradaoperacao")), "yyyy-MM-dd").alias("dat_entrada_operacao"),
        F.to_date(F.trim(F.col("dat_desativacao")), "yyyy-MM-dd").alias("dat_desativacao"),
        decimal_limpo("val_potenciaefetiva").alias("potencia_efetiva_mw"),
        F.col("_ingest_ts"),
    )
    # Uma unidade geradora sem potência não agrega nada à dimensão de usina.
    .filter(F.col("potencia_efetiva_mw").isNotNull())
    .withColumn("esta_ativa", F.col("dat_desativacao").isNull())
)

salvar_silver(
    df_capacidade,
    "capacidade_instalada_ug",
    "SILVER — Capacidade instalada por unidade geradora, limpa e tipada. Granularidade: 1 linha "
    "por unidade geradora (cod_equipamento). esta_ativa=false indica unidade já desativada. "
    "Todos os campos de texto passaram por TRIM — a fonte publica nom_subsistema com espaços "
    "à direita. Linhagem: bronze.capacidade_geracao.",
    chave=["cod_equipamento", "nom_usina"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência da camada

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SILVER()}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Evidência de que tratei o problema 5 (linha agregada `SIN`): o total marcado como
# MAGIC agregado deve bater com a soma dos quatro subsistemas reais.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            eh_agregado,
            COUNT(DISTINCT id_subsistema)          AS qtd_subsistemas,
            ROUND(SUM(carga_mwmed) / 1e6, 2)       AS carga_total_tw_med,
            MIN(din_instante)                      AS inicio,
            MAX(din_instante)                      AS fim
        FROM {SILVER('balanco_subsistema_horario')}
        GROUP BY eh_agregado
        ORDER BY eh_agregado
    """)
)

# COMMAND ----------

print("Camada Silver concluída. Próximo passo: notebook `03_gold_modelagem`.")
