# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Gold: modelagem dimensional
# MAGIC
# MAGIC **Etapa do MVP:** 4.3 (Modelagem) e 4.4 (Carga/ETL)
# MAGIC
# MAGIC ## Modelo que escolhi: constelação de fatos (star schema com dimensões conformes)
# MAGIC
# MAGIC As perguntas do objetivo vivem em **três granularidades diferentes**: geração por hora,
# MAGIC restrição por usina e nível de reservatório por dia. Um único fato não comporta isso sem
# MAGIC inventar linhas ou perder detalhe. A solução clássica é uma constelação: vários fatos
# MAGIC compartilhando as mesmas dimensões.
# MAGIC
# MAGIC ```
# MAGIC                    dim_data ──────┬──────── dim_hora
# MAGIC                        │          │             │
# MAGIC        ┌───────────────┼──────────┼─────────────┼───────────────┐
# MAGIC        │               │          │             │               │
# MAGIC  fato_reservatorio  fato_balanco_horario   fato_geracao_horaria  fato_restricao_horaria
# MAGIC     _diario              │                      │                    │      │
# MAGIC        │                 │                      │                    │      │
# MAGIC        └────────── dim_subsistema ──────────────┘                    │   dim_usina
# MAGIC                          │                                           │      │
# MAGIC                          └──────── dim_fonte_geracao ────────────────┘──────┘
# MAGIC ```
# MAGIC
# MAGIC ### Por que separei `dim_data` e `dim_hora`
# MAGIC Uma dimensão de tempo horária única teria ~50 mil linhas e ainda assim não serviria ao fato
# MAGIC diário de reservatório. Separando data (grão dia) de hora-do-dia (24 linhas), os quatro
# MAGIC fatos convivem: os horários usam as duas chaves, o diário usa só `sk_data`.
# MAGIC
# MAGIC ### A transformação estrutural: UNPIVOT
# MAGIC O balanço do ONS é **largo** — uma coluna por fonte (`val_gerhidraulica`, `val_gereolica`…).
# MAGIC Nesse formato, "qual fonte cresceu mais?" exige reescrever a query a cada fonte nova.
# MAGIC `fato_geracao_horaria` despivota isso para o formato **longo** (uma linha por fonte), que é
# MAGIC o que torna `dim_fonte_geracao` utilizável como dimensão de verdade.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

def documentar(tabela: str, descricao: str, colunas: dict) -> None:
    """Grava a descrição da tabela e de cada coluna no Unity Catalog.

    Este é o catálogo de dados exigido pela etapa 4.3. Escrevendo os comentários aqui, no
    mesmo código que cria a tabela, a documentação não pode divergir do modelo — ela é
    gerada pelo próprio pipeline e fica visível na UI do Unity Catalog.
    """
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{esc_sql(descricao)}'")
    for coluna, comentario in colunas.items():
        texto = comentario.replace("'", "''")
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{texto}'")
    print(f"     catálogo: {len(colunas)} colunas documentadas")


def salvar_gold(df: DataFrame, nome: str, descricao: str, colunas: dict) -> None:
    tabela = GOLD(nome)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
    n = spark.table(tabela).count()
    print(f"[OK] {tabela:<42} {n:>12,} linhas")
    documentar(tabela, descricao, colunas)

# COMMAND ----------

# MAGIC %md
# MAGIC # DIMENSÕES

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_data`
# MAGIC
# MAGIC Construí esta dimensão a partir do intervalo observado nos fatos, não de um calendário arbitrário.
# MAGIC `estacao_ano` usa o **hemisfério sul** — o que importa aqui, já que sazonalidade de vento
# MAGIC (forte no inverno/primavera no Nordeste) e de chuva são centrais para P2 e P6.

# COMMAND ----------

# O intervalo precisa cobrir as datas de TODOS os fatos. Na primeira versão eu usava só o
# balanço, e o EAR — publicado com um dia a mais — gerou 4 chaves órfãs na verificação de
# integridade referencial.
limites = spark.sql(f"""
    WITH datas AS (
        SELECT CAST(din_instante AS DATE) AS d FROM {SILVER('balanco_subsistema_horario')}
        UNION ALL
        SELECT dat_referencia FROM {SILVER('ear_subsistema_diario')}
        UNION ALL
        SELECT CAST(din_instante AS DATE) FROM {SILVER('restricao_coff_usina')}
    )
    SELECT MIN(d) AS ini, MAX(d) AS fim FROM datas
""").collect()[0]
print(f"Intervalo observado nos fatos: {limites['ini']} a {limites['fim']}")

df_data = (
    spark.sql(
        f"SELECT explode(sequence(DATE'{limites['ini']}', DATE'{limites['fim']}', "
        f"INTERVAL 1 DAY)) AS data"
    )
    .withColumn("sk_data", F.date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("dia_do_ano", F.dayofyear("data"))
    .withColumn("num_dia_semana", F.dayofweek("data"))  # 1=domingo ... 7=sábado
    .withColumn("eh_fim_semana", F.col("num_dia_semana").isin([1, 7]))
    .withColumn(
        "nom_mes",
        F.element_at(
            F.array(*[F.lit(m) for m in [
                "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
                "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]]),
            F.col("mes"),
        ),
    )
    .withColumn(
        "nom_dia_semana",
        F.element_at(
            F.array(*[F.lit(d) for d in [
                "Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]]),
            F.col("num_dia_semana"),
        ),
    )
    # Estações do hemisfério sul, por dia do ano.
    .withColumn(
        "estacao_ano",
        F.when((F.col("dia_do_ano") >= 355) | (F.col("dia_do_ano") < 80), "Verão")
        .when(F.col("dia_do_ano") < 172, "Outono")
        .when(F.col("dia_do_ano") < 266, "Inverno")
        .otherwise("Primavera"),
    )
    .select(
        "sk_data", "data", "ano", "mes", "nom_mes", "dia", "ano_mes", "trimestre",
        "num_dia_semana", "nom_dia_semana", "eh_fim_semana", "dia_do_ano", "estacao_ano",
    )
)

salvar_gold(
    df_data,
    "dim_data",
    "GOLD — Dimensão de calendário no grão DIA, cobrindo o intervalo observado nos fatos. "
    "Dimensão conforme, compartilhada pelos quatro fatos do modelo. Linhagem: gerada pelo "
    "pipeline a partir do intervalo de datas das tabelas Silver de balanço, EAR e restrição.",
    {
        "sk_data": "Chave substituta (surrogate key) no formato inteiro AAAAMMDD. Ex.: 20250131. Domínio: 20210101 a 20261231.",
        "data": "Data civil correspondente. Tipo DATE.",
        "ano": "Ano com 4 dígitos. Domínio: 2021 a 2026.",
        "mes": "Número do mês. Domínio: 1 a 12.",
        "nom_mes": "Nome do mês por extenso em português. Domínio: Janeiro..Dezembro.",
        "dia": "Dia do mês. Domínio: 1 a 31.",
        "ano_mes": "Competência no formato AAAA-MM, para agregação e ordenação mensal.",
        "trimestre": "Trimestre civil. Domínio: 1 a 4.",
        "num_dia_semana": "Número do dia da semana no padrão Spark. Domínio: 1 (domingo) a 7 (sábado).",
        "nom_dia_semana": "Nome do dia da semana em português. Domínio: Domingo..Sábado.",
        "eh_fim_semana": "Booleano: verdadeiro para sábado e domingo. Usado para separar perfis de carga.",
        "dia_do_ano": "Dia sequencial do ano. Domínio: 1 a 366.",
        "estacao_ano": "Estação no HEMISFÉRIO SUL, derivada do dia do ano. Domínio: Verão, Outono, Inverno, Primavera.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_hora`
# MAGIC
# MAGIC 24 linhas. Pequena, mas é ela que permite responder P2 e P7 sem espalhar regra de negócio
# MAGIC pelas queries: a definição de "horário de ponta" e de "período do dia" mora aqui, num lugar
# MAGIC só, e toda análise herda a mesma definição.

# COMMAND ----------

df_hora = (
    spark.range(0, 24)
    .withColumnRenamed("id", "sk_hora")
    .withColumn("hora", F.col("sk_hora").cast("int"))
    .withColumn("hora_rotulo", F.format_string("%02dh", F.col("hora")))
    .withColumn(
        "periodo_dia",
        F.when(F.col("hora") < 6, "Madrugada")
        .when(F.col("hora") < 12, "Manhã")
        .when(F.col("hora") < 18, "Tarde")
        .otherwise("Noite"),
    )
    # Ponta convencional do setor elétrico brasileiro: 18h-21h.
    .withColumn("eh_horario_ponta", (F.col("hora") >= 18) & (F.col("hora") <= 20))
    # Janela em que a geração solar é fisicamente possível — usada para
    # distinguir "solar zerada por ser noite" de "solar zerada por corte".
    .withColumn("eh_janela_solar", (F.col("hora") >= 6) & (F.col("hora") <= 18))
    .withColumn("sk_hora", F.col("sk_hora").cast("int"))
    .select("sk_hora", "hora", "hora_rotulo", "periodo_dia", "eh_horario_ponta", "eh_janela_solar")
)

salvar_gold(
    df_hora,
    "dim_hora",
    "GOLD — Dimensão de hora do dia (24 linhas). Dimensão conforme, compartilhada pelos três "
    "fatos horários. Centraliza as definições de período do dia, horário de ponta e janela "
    "solar. Linhagem: gerada pelo pipeline.",
    {
        "sk_hora": "Chave substituta = a própria hora do dia. Domínio: 0 a 23.",
        "hora": "Hora do dia em formato inteiro. Domínio: 0 a 23.",
        "hora_rotulo": "Rótulo da hora para exibição em gráficos. Domínio: '00h' a '23h'.",
        "periodo_dia": "Faixa do dia. Domínio: Madrugada (0-5), Manhã (6-11), Tarde (12-17), Noite (18-23).",
        "eh_horario_ponta": "Booleano: verdadeiro entre 18h e 20h, o horário de ponta convencional do setor elétrico brasileiro.",
        "eh_janela_solar": "Booleano: verdadeiro entre 6h e 18h, janela em que há irradiação. Distingue solar zerada por ausência de sol de solar zerada por restrição.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_subsistema`
# MAGIC
# MAGIC Dimensão pequena e estável, que defini em `_config` a partir da definição operativa do SIN.
# MAGIC A flag `eh_agregado` carrega para o modelo dimensional o tratamento do problema de dupla
# MAGIC contagem que identifiquei no Silver.

# COMMAND ----------

df_subsistema = spark.createDataFrame(
    SUBSISTEMAS, ["id_subsistema", "nom_subsistema_ons", "nom_subsistema", "estados", "eh_agregado"]
).withColumn("sk_subsistema", F.col("id_subsistema"))

salvar_gold(
    df_subsistema.select(
        "sk_subsistema", "id_subsistema", "nom_subsistema", "nom_subsistema_ons",
        "estados", "eh_agregado",
    ),
    "dim_subsistema",
    "GOLD — Dimensão de subsistema elétrico do SIN. Dimensão conforme, compartilhada pelos "
    "quatro fatos. Linhagem: definida no pipeline (notebook _config) a partir da definição "
    "operativa do Sistema Interligado Nacional publicada pelo ONS.",
    {
        "sk_subsistema": "Chave substituta = sigla do subsistema. Domínio: N, NE, SE, S, SIN.",
        "id_subsistema": "Sigla do subsistema conforme publicada pelo ONS. Domínio: N, NE, SE, S, SIN.",
        "nom_subsistema": "Nome curto para exibição. Domínio: Norte, Nordeste, Sudeste/C.O., Sul, Brasil.",
        "nom_subsistema_ons": "Nome completo exatamente como o ONS publica nos arquivos de origem.",
        "estados": "Unidades da federação abrangidas pelo subsistema, separadas por vírgula.",
        "eh_agregado": "Booleano CRÍTICO: verdadeiro apenas para SIN, que é o TOTAL NACIONAL e não um subsistema. Filtre eh_agregado = false ao somar por subsistema, sob pena de dupla contagem.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_fonte_geracao`
# MAGIC
# MAGIC Os três atributos booleanos são o que dá poder analítico a esta dimensão. `eh_intermitente`
# MAGIC separa o que depende de recurso natural não controlável (eólica e solar) do que pode ser
# MAGIC despachado — a distinção central de todo o problema investigado.

# COMMAND ----------

df_fonte = spark.createDataFrame(
    FONTES_GERACAO,
    ["cod_fonte", "nom_fonte", "coluna_origem", "eh_renovavel", "eh_despachavel", "eh_intermitente"],
).withColumn("sk_fonte", F.col("cod_fonte"))

salvar_gold(
    df_fonte.select(
        "sk_fonte", "cod_fonte", "nom_fonte", "eh_renovavel", "eh_despachavel",
        "eh_intermitente", "coluna_origem",
    ),
    "dim_fonte_geracao",
    "GOLD — Dimensão de fonte de geração. Dimensão conforme, usada por fato_geracao_horaria e "
    "fato_restricao_horaria. Linhagem: definida no pipeline (notebook _config); coluna_origem "
    "registra de qual coluna do arquivo de balanço do ONS cada fonte foi despivotada.",
    {
        "sk_fonte": "Chave substituta = código da fonte. Domínio: HID, TER, EOL, SOL.",
        "cod_fonte": "Código curto da fonte de geração. Domínio: HID, TER, EOL, SOL.",
        "nom_fonte": "Nome da fonte por extenso. Domínio: Hidráulica, Térmica, Eólica, Solar.",
        "eh_renovavel": "Booleano: verdadeiro para hidráulica, eólica e solar; falso para térmica.",
        "eh_despachavel": "Booleano: verdadeiro quando o operador controla o momento da geração (hidráulica e térmica).",
        "eh_intermitente": "Booleano: verdadeiro para eólica e solar, cuja geração depende de recurso natural não controlável. Atributo central das perguntas P2, P3 e P7.",
        "coluna_origem": "Rastreabilidade: nome da coluna no arquivo de balanço do ONS que originou esta fonte no UNPIVOT.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.dim_usina`
# MAGIC
# MAGIC Única dimensão que construí por **JOIN entre duas fontes distintas**:
# MAGIC
# MAGIC - `silver.restricao_coff_usina` fornece a lista de usinas com dados de restrição
# MAGIC - `silver.capacidade_instalada_ug` fornece a potência instalada, somada por código CEG
# MAGIC
# MAGIC Na primeira versão fiz este JOIN pelo nome da usina, e ele casou **0 de 248** usinas. Havia
# MAGIC dois motivos, e o `TRIM` não resolvia nenhum deles:
# MAGIC
# MAGIC 1. **Caixa diferente** — a restrição publica `Conj. Olho d'Água`; o cadastro de capacidade, `CAMILO PONTES II`.
# MAGIC 2. **Granularidade diferente** — 233 das 248 entradas da restrição são *conjuntos de usinas*,
# MAGIC    que não existem como linha no cadastro, que é por unidade geradora.
# MAGIC
# MAGIC Por isso passei a fazer o JOIN pelo **código CEG**, que é o identificador oficial da ANEEL. Ele
# MAGIC casa as 15 usinas individuais. Os 233 conjuntos ficam com `potencia_instalada_mw` nula por
# MAGIC construção — o dicionário do ONS confirma que conjuntos não têm CEG. Registro isso na coluna
# MAGIC `tem_cadastro_capacidade`, em vez de esconder a limitação.

# COMMAND ----------

# Uma linha por id_ons. Se a usina mudar de nome ou de atributo ao longo do período, agrupar
# por todos os atributos geraria mais de uma linha por chave e quebraria a PK da dimensão.
# Por isso fico com os atributos do registro mais recente (max_by).
usinas_restricao = (
    spark.table(SILVER("restricao_coff_usina"))
    .groupBy("id_ons")
    .agg(
        F.max_by("nom_usina", "din_instante").alias("nom_usina"),
        F.max_by("ceg", "din_instante").alias("ceg"),
        F.max_by("cod_fonte", "din_instante").alias("cod_fonte"),
        F.max_by("nom_fonte", "din_instante").alias("nom_fonte"),
        F.max_by("id_estado", "din_instante").alias("id_estado"),
        F.max_by("nom_estado", "din_instante").alias("nom_estado"),
        F.max_by("id_subsistema", "din_instante").alias("id_subsistema"),
        F.min("din_instante").alias("primeiro_registro"),
        F.max("din_instante").alias("ultimo_registro"),
        F.max("disponibilidade_mwmed").alias("disponibilidade_maxima_mwmed"),
    )
)

capacidade_por_usina = (
    spark.table(SILVER("capacidade_instalada_ug"))
    .filter(F.col("esta_ativa") & F.col("ceg").isNotNull())
    .groupBy("ceg")
    .agg(
        F.round(F.sum("potencia_efetiva_mw"), 3).alias("potencia_instalada_mw"),
        F.count("*").alias("qtd_unidades_geradoras"),
        F.min("dat_entrada_operacao").alias("dat_entrada_operacao"),
    )
)

df_usina = (
    usinas_restricao.join(capacidade_por_usina, on="ceg", how="left")
    .withColumn("sk_usina", F.col("id_ons"))
    .withColumn("tem_cadastro_capacidade", F.col("potencia_instalada_mw").isNotNull())
    .select(
        "sk_usina", "id_ons", "nom_usina", "ceg", "cod_fonte", "nom_fonte", "id_estado", "nom_estado",
        "id_subsistema", "potencia_instalada_mw", "qtd_unidades_geradoras",
        "dat_entrada_operacao", "disponibilidade_maxima_mwmed", "tem_cadastro_capacidade",
        "primeiro_registro", "ultimo_registro",
    )
)

salvar_gold(
    df_usina,
    "dim_usina",
    "GOLD — Dimensão de usina eólica e fotovoltaica com registro de constrained-off. "
    "Granularidade: 1 linha por usina (id_ons). Linhagem: JOIN entre "
    "silver.restricao_coff_usina (lista de usinas e localização) e "
    "silver.capacidade_instalada_ug (potência instalada somada por CEG), pelo código CEG.",
    {
        "sk_usina": "Chave substituta = código da usina no ONS (id_ons).",
        "id_ons": "Código identificador da usina no ONS. Chave natural.",
        "nom_usina": "Nome da usina ou conjunto conforme publicado pelo ONS. Prefixo 'Conj.' indica conjunto de usinas tratado como unidade operativa.",
        "ceg": "Código Único do Empreendimento de Geração (ANEEL). NULO para conjuntos de usinas, que não têm CEG (no arquivo do ONS vêm como '-'). Chave do JOIN com o cadastro de capacidade.",
        "cod_fonte": "Fonte de geração da usina. Domínio: EOL, SOL.",
        "nom_fonte": "Fonte por extenso. Domínio: Eólica, Solar.",
        "id_estado": "Sigla da UF onde a usina está instalada. Domínio: as 27 UFs.",
        "nom_estado": "Nome da UF por extenso.",
        "id_subsistema": "Subsistema ao qual a usina pertence. Domínio: N, NE, SE, S.",
        "potencia_instalada_mw": "Soma da potência efetiva das unidades geradoras ativas com o mesmo CEG, em MW. NULO para conjuntos de usinas, que não têm CEG e portanto não casam com o cadastro de capacidade.",
        "qtd_unidades_geradoras": "Quantidade de unidades geradoras ativas somadas na potência instalada.",
        "dat_entrada_operacao": "Data de entrada em operação da unidade geradora mais antiga da usina.",
        "disponibilidade_maxima_mwmed": "Maior disponibilidade já registrada para a usina no período, em MWmed. Proxy de porte quando não há cadastro de capacidade.",
        "tem_cadastro_capacidade": "Booleano: indica se o JOIN com o cadastro de capacidade encontrou correspondência. Falso sinaliza cobertura incompleta, não erro.",
        "primeiro_registro": "Instante do primeiro registro de restrição da usina no período coletado.",
        "ultimo_registro": "Instante do último registro de restrição da usina no período coletado.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC # FATOS

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.fato_geracao_horaria` — o UNPIVOT
# MAGIC
# MAGIC **Grão:** 1 linha por subsistema, por hora, por fonte.
# MAGIC
# MAGIC Transforma o balanço largo em formato longo. Quatro colunas de geração viram quatro linhas,
# MAGIC ligadas a `dim_fonte_geracao`. É o fato que responde P1 diretamente.

# COMMAND ----------

balanco = spark.table(SILVER("balanco_subsistema_horario")).filter(~F.col("eh_agregado"))

df_geracao = (
    balanco.select(
        "id_subsistema",
        "din_instante",
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(cod).alias("cod_fonte"),
                        F.coalesce(F.col(col_silver), F.lit(0.0)).alias("geracao_mwmed"),
                    )
                    for cod, col_silver in [
                        ("HID", "ger_hidraulica_mwmed"),
                        ("TER", "ger_termica_mwmed"),
                        ("EOL", "ger_eolica_mwmed"),
                        ("SOL", "ger_solar_mwmed"),
                    ]
                ]
            )
        ).alias("fonte"),
    )
    .select(
        F.date_format("din_instante", "yyyyMMdd").cast("int").alias("sk_data"),
        F.hour("din_instante").alias("sk_hora"),
        F.col("id_subsistema").alias("sk_subsistema"),
        F.col("fonte.cod_fonte").alias("sk_fonte"),
        "din_instante",
        F.round(F.col("fonte.geracao_mwmed"), 4).alias("geracao_mwmed"),
    )
    # Intervalo de 1 hora: energia em MWh é numericamente igual à potência média.
    .withColumn("geracao_mwh", F.col("geracao_mwmed"))
)

salvar_gold(
    df_geracao,
    "fato_geracao_horaria",
    "GOLD — Fato de geração por fonte. Grão: 1 linha por subsistema, por hora, por fonte de "
    "geração. Resultado do UNPIVOT das quatro colunas de geração do balanço do ONS. Exclui a "
    "linha agregada SIN para evitar dupla contagem. Linhagem: silver.balanco_subsistema_horario.",
    {
        "sk_data": "FK para gold.dim_data. Formato AAAAMMDD.",
        "sk_hora": "FK para gold.dim_hora. Domínio: 0 a 23.",
        "sk_subsistema": "FK para gold.dim_subsistema. Domínio: N, NE, SE, S (SIN excluído).",
        "sk_fonte": "FK para gold.dim_fonte_geracao. Domínio: HID, TER, EOL, SOL.",
        "din_instante": "Timestamp da hora cheia de referência, mantido para ordenação e séries temporais.",
        "geracao_mwmed": "MÉTRICA ADITIVA: geração média verificada no intervalo de 1 hora, em MW médios. Domínio: >= 0.",
        "geracao_mwh": "MÉTRICA ADITIVA: energia gerada na hora, em MWh. Igual a geracao_mwmed por o intervalo ser de 1 hora.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.fato_balanco_horario`
# MAGIC
# MAGIC **Grão:** 1 linha por subsistema, por hora.
# MAGIC
# MAGIC Mantenho em formato largo de propósito: `carga`, `intercâmbio` e `carga líquida` **não são
# MAGIC fontes de geração** e não caberiam no UNPIVOT sem distorcer o grão. É o fato que responde
# MAGIC P2 e P7.

# COMMAND ----------

df_balanco_fato = balanco.select(
    F.date_format("din_instante", "yyyyMMdd").cast("int").alias("sk_data"),
    F.hour("din_instante").alias("sk_hora"),
    F.col("id_subsistema").alias("sk_subsistema"),
    "din_instante",
    F.round("carga_mwmed", 4).alias("carga_mwmed"),
    F.round("ger_total_mwmed", 4).alias("ger_total_mwmed"),
    F.round("ger_intermitente_mwmed", 4).alias("ger_intermitente_mwmed"),
    F.round("carga_liquida_mwmed", 4).alias("carga_liquida_mwmed"),
    F.round("intercambio_mwmed", 4).alias("intercambio_mwmed"),
    "pct_intermitente_sobre_carga",
).withColumn("carga_liquida_negativa", F.col("carga_liquida_mwmed") < 0)

salvar_gold(
    df_balanco_fato,
    "fato_balanco_horario",
    "GOLD — Fato de balanço energético. Grão: 1 linha por subsistema, por hora. Contém carga, "
    "geração total, geração intermitente, carga líquida e intercâmbio. Exclui a linha agregada "
    "SIN. Linhagem: silver.balanco_subsistema_horario.",
    {
        "sk_data": "FK para gold.dim_data. Formato AAAAMMDD.",
        "sk_hora": "FK para gold.dim_hora. Domínio: 0 a 23.",
        "sk_subsistema": "FK para gold.dim_subsistema. Domínio: N, NE, SE, S (SIN excluído).",
        "din_instante": "Timestamp da hora cheia de referência.",
        "carga_mwmed": "MÉTRICA ADITIVA: carga (demanda) verificada do subsistema na hora, em MW médios.",
        "ger_total_mwmed": "MÉTRICA ADITIVA: soma da geração de todas as quatro fontes na hora, em MW médios.",
        "ger_intermitente_mwmed": "MÉTRICA ADITIVA: geração eólica + solar na hora, em MW médios.",
        "carga_liquida_mwmed": "MÉTRICA ADITIVA: carga menos geração intermitente. É a demanda que precisa ser atendida por fontes despacháveis. Pode ser NEGATIVA quando a geração intermitente supera a carga local.",
        "intercambio_mwmed": "MÉTRICA ADITIVA: intercâmbio líquido do subsistema, em MW médios. Positivo indica exportação para outros subsistemas; negativo, importação.",
        "pct_intermitente_sobre_carga": "MÉTRICA NÃO ADITIVA (razão): participação percentual da geração intermitente sobre a carga. Não somar entre linhas; recalcular a partir das somas. Domínio típico: 0 a 200+ em subsistemas exportadores.",
        "carga_liquida_negativa": "Booleano: verdadeiro quando a geração intermitente sozinha supera toda a carga do subsistema naquela hora.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.fato_restricao_horaria`
# MAGIC
# MAGIC **Grão:** 1 linha por usina, por hora.
# MAGIC
# MAGIC A fonte é semi-horária (30 min). A agregação para hora cheia é uma decisão minha: alinha este fato
# MAGIC ao grão dos demais, permitindo cruzar corte com balanço energético na mesma query. Somo as duas
# MAGIC medidas de meia hora em MWh (energia é aditiva) e preservo o número de intervalos
# MAGIC com restrição em `intervalos_com_restricao`, para não perder a informação de
# MAGIC duração. É o fato que responde P3, P4, P5 e P8.

# COMMAND ----------

restricao = spark.table(SILVER("restricao_coff_usina"))

df_restricao_fato = (
    restricao.withColumn("hora_cheia", F.date_trunc("hour", F.col("din_instante")))
    .groupBy("id_ons", "cod_fonte", "id_subsistema", "hora_cheia")
    .agg(
        F.round(F.sum("corte_mwh"), 6).alias("corte_mwh"),
        F.round(F.sum("geracao_mwh"), 6).alias("geracao_mwh"),
        F.round(F.sum("geracao_referencia_mwh"), 6).alias("geracao_referencia_mwh"),
        F.sum(F.col("houve_restricao").cast("int")).alias("intervalos_com_restricao"),
        F.count("*").alias("intervalos_no_periodo"),
        # Energia cortada separada por razão e por origem. Somando por categoria, cada meia hora
        # contribui para a razão que de fato tinha; atribuir a hora inteira a uma única razão
        # distorceria a P5 sempre que as duas meias horas tivessem razões diferentes.
        *[
            F.round(F.sum(F.when(F.col("cod_razao_restricao") == cod, F.col("corte_mwh")).otherwise(0.0)), 6)
            .alias("corte_%s_mwh" % cod.lower())
            for cod in ["REL", "CNF", "ENE", "PAR"]
        ],
        *[
            F.round(F.sum(F.when(F.col("cod_origem_restricao") == cod, F.col("corte_mwh")).otherwise(0.0)), 6)
            .alias("corte_%s_mwh" % cod.lower())
            for cod in ["SIS", "LOC"]
        ],
        F.round(F.sum("gnra_oficial_mwh"), 6).alias("gnra_oficial_mwh"),
        # Razão mais frequente na hora — só para filtro e leitura, nunca para somar energia.
        F.mode("cod_razao_restricao").alias("cod_razao_predominante"),
    )
    .select(
        F.date_format("hora_cheia", "yyyyMMdd").cast("int").alias("sk_data"),
        F.hour("hora_cheia").alias("sk_hora"),
        F.col("id_ons").alias("sk_usina"),
        F.col("id_subsistema").alias("sk_subsistema"),
        F.col("cod_fonte").alias("sk_fonte"),
        F.col("hora_cheia").alias("din_instante"),
        "corte_mwh",
        "geracao_mwh",
        "geracao_referencia_mwh",
        "intervalos_com_restricao",
        "intervalos_no_periodo",
        "corte_rel_mwh",
        "corte_cnf_mwh",
        "corte_ene_mwh",
        "corte_par_mwh",
        "corte_sis_mwh",
        "corte_loc_mwh",
        "gnra_oficial_mwh",
        "cod_razao_predominante",
    )
    .withColumn("houve_restricao_na_hora", F.col("intervalos_com_restricao") > 0)
    .withColumn(
        "taxa_corte_pct",
        F.when(
            F.col("geracao_referencia_mwh") > 0,
            F.round(100 * F.col("corte_mwh") / F.col("geracao_referencia_mwh"), 4),
        ),
    )
)

salvar_gold(
    df_restricao_fato,
    "fato_restricao_horaria",
    "GOLD — Fato de restrição de operação (constrained-off) de usinas eólicas e fotovoltaicas. "
    "Grão: 1 linha por usina, por hora. Agregado a partir do grão semi-horário da fonte, "
    "somando energia (aditiva) e preservando a contagem de intervalos restritos. "
    "Linhagem: silver.restricao_coff_usina.",
    {
        "sk_data": "FK para gold.dim_data. Formato AAAAMMDD.",
        "sk_hora": "FK para gold.dim_hora. Domínio: 0 a 23.",
        "sk_usina": "FK para gold.dim_usina (id_ons da usina).",
        "sk_subsistema": "FK para gold.dim_subsistema. Domínio: N, NE, SE, S.",
        "sk_fonte": "FK para gold.dim_fonte_geracao. Domínio: EOL, SOL.",
        "din_instante": "Timestamp da hora cheia de referência.",
        "corte_mwh": "MÉTRICA ADITIVA: energia cortada por constrained-off na hora, em MWh. Calculada como MAX(geracao_referencia - geracao, 0) x 0,5h somada nos intervalos de 30 min, apenas quando há razão de restrição declarada. Domínio: >= 0.",
        "geracao_mwh": "MÉTRICA ADITIVA: energia efetivamente gerada pela usina na hora, em MWh.",
        "geracao_referencia_mwh": "MÉTRICA ADITIVA: energia que a usina teria gerado com o recurso disponível, sem restrição, em MWh.",
        "intervalos_com_restricao": "MÉTRICA ADITIVA: quantidade de intervalos de 30 min da hora em que havia razão de restrição declarada. Domínio: 0 a 2.",
        "intervalos_no_periodo": "MÉTRICA ADITIVA: quantidade de intervalos de 30 min presentes na fonte para a hora. Esperado 2; valor menor indica lacuna na origem.",
        "corte_rel_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh em intervalos com razão REL (indisponibilidade externa elétrica), em MWh.",
        "corte_cnf_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh em intervalos com razão CNF (atendimento a requisitos de confiabilidade), em MWh.",
        "corte_ene_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh em intervalos com razão ENE (razão energética), em MWh.",
        "corte_par_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh em intervalos com razão PAR (restrição indicada no parecer de acesso), em MWh.",
        "corte_sis_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh com origem sistêmica (SIS), em MWh.",
        "corte_loc_mwh": "MÉTRICA ADITIVA: parcela de corte_mwh com origem local (LOC), em MWh.",
        "gnra_oficial_mwh": "MÉTRICA ADITIVA: Geração Não Realizada Apurada publicada pelo ONS (val_geracaonaorealizadaapurada x 0,5h), em MWh. Usada para validar corte_mwh.",
        "cod_razao_predominante": "Razão de restrição mais frequente entre os intervalos da hora. Domínio: REL, CNF, ENE, PAR, NULO. Apenas para leitura; para somar energia por razão use as colunas corte_<razao>_mwh.",
        "houve_restricao_na_hora": "Booleano: verdadeiro se ao menos um intervalo de 30 min da hora teve restrição.",
        "taxa_corte_pct": "MÉTRICA NÃO ADITIVA (razão): percentual da geração de referência que foi cortado. Não somar entre linhas; recalcular a partir das somas. Domínio: 0 a 100.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `gold.fato_reservatorio_diario`
# MAGIC
# MAGIC **Grão:** 1 linha por subsistema, por dia. É o fato que sustenta P6.

# COMMAND ----------

df_reservatorio = (
    spark.table(SILVER("ear_subsistema_diario"))
    .filter(~F.col("eh_agregado"))
    .select(
        F.date_format("dat_referencia", "yyyyMMdd").cast("int").alias("sk_data"),
        F.col("id_subsistema").alias("sk_subsistema"),
        F.col("dat_referencia"),
        F.round("ear_verificada_mwmes", 4).alias("ear_verificada_mwmes"),
        F.round("ear_maxima_mwmes", 4).alias("ear_maxima_mwmes"),
        F.round("ear_percentual", 4).alias("ear_percentual"),
    )
    .withColumn(
        "faixa_armazenamento",
        F.when(F.col("ear_percentual") < 30, "Crítico (<30%)")
        .when(F.col("ear_percentual") < 50, "Baixo (30-50%)")
        .when(F.col("ear_percentual") < 70, "Confortável (50-70%)")
        .otherwise("Alto (>70%)"),
    )
)

salvar_gold(
    df_reservatorio,
    "fato_reservatorio_diario",
    "GOLD — Fato de energia armazenada nos reservatórios (EAR). Grão: 1 linha por subsistema, "
    "por dia. Exclui a linha agregada SIN. Linhagem: silver.ear_subsistema_diario.",
    {
        "sk_data": "FK para gold.dim_data. Formato AAAAMMDD.",
        "sk_subsistema": "FK para gold.dim_subsistema. Domínio: N, NE, SE, S (SIN excluído).",
        "dat_referencia": "Data civil de referência da medição.",
        "ear_verificada_mwmes": "MÉTRICA SEMI-ADITIVA: energia armazenada verificada nos reservatórios, em MWmês. É um estoque: aditiva entre subsistemas, NÃO aditiva ao longo do tempo (usar média ou último valor).",
        "ear_maxima_mwmes": "MÉTRICA SEMI-ADITIVA: capacidade máxima de armazenamento do subsistema, em MWmês.",
        "ear_percentual": "MÉTRICA NÃO ADITIVA (razão): nível de armazenamento como percentual da capacidade máxima. Domínio: 0 a 100.",
        "faixa_armazenamento": "Faixa categórica derivada de ear_percentual. Domínio: 'Crítico (<30%)', 'Baixo (30-50%)', 'Confortável (50-70%)', 'Alto (>70%)'.",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência do modelo
# MAGIC
# MAGIC ### Integridade referencial
# MAGIC O Unity Catalog aceita declarar PK/FK, mas **não as impõe** (são informativas, e ajudam o
# MAGIC otimizador e o diagrama de linhagem). Então faço a verificação abaixo por query: toda
# MAGIC chave dos fatos precisa existir na dimensão correspondente. Qualquer valor diferente de
# MAGIC zero indica órfão no modelo.

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'fato_geracao_horaria -> dim_data' AS verificacao,
           COUNT(*) AS chaves_orfas
      FROM {GOLD('fato_geracao_horaria')} f
      LEFT ANTI JOIN {GOLD('dim_data')} d ON f.sk_data = d.sk_data
    UNION ALL
    SELECT 'fato_geracao_horaria -> dim_fonte_geracao',
           COUNT(*)
      FROM {GOLD('fato_geracao_horaria')} f
      LEFT ANTI JOIN {GOLD('dim_fonte_geracao')} d ON f.sk_fonte = d.sk_fonte
    UNION ALL
    SELECT 'fato_balanco_horario -> dim_subsistema',
           COUNT(*)
      FROM {GOLD('fato_balanco_horario')} f
      LEFT ANTI JOIN {GOLD('dim_subsistema')} d ON f.sk_subsistema = d.sk_subsistema
    UNION ALL
    SELECT 'fato_restricao_horaria -> dim_usina',
           COUNT(*)
      FROM {GOLD('fato_restricao_horaria')} f
      LEFT ANTI JOIN {GOLD('dim_usina')} d ON f.sk_usina = d.sk_usina
    UNION ALL
    SELECT 'fato_restricao_horaria -> dim_data',
           COUNT(*)
      FROM {GOLD('fato_restricao_horaria')} f
      LEFT ANTI JOIN {GOLD('dim_data')} d ON f.sk_data = d.sk_data
    UNION ALL
    SELECT 'fato_reservatorio_diario -> dim_data',
           COUNT(*)
      FROM {GOLD('fato_reservatorio_diario')} f
      LEFT ANTI JOIN {GOLD('dim_data')} d ON f.sk_data = d.sk_data
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaração de chaves primárias e estrangeiras
# MAGIC
# MAGIC Declarar as constraints faz o Unity Catalog desenhar o relacionamento entre as tabelas na
# MAGIC aba de linhagem — é o que transforma o modelo em documentação navegável, exigida na etapa 4.3.
# MAGIC Constraints exigem colunas `NOT NULL`, então a alteração vem antes.

# COMMAND ----------

chaves = [
    (GOLD("dim_data"), "sk_data", "pk_dim_data"),
    (GOLD("dim_hora"), "sk_hora", "pk_dim_hora"),
    (GOLD("dim_subsistema"), "sk_subsistema", "pk_dim_subsistema"),
    (GOLD("dim_fonte_geracao"), "sk_fonte", "pk_dim_fonte"),
    (GOLD("dim_usina"), "sk_usina", "pk_dim_usina"),
]

for tabela, coluna, nome_pk in chaves:
    try:
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} SET NOT NULL")
        spark.sql(f"ALTER TABLE {tabela} ADD CONSTRAINT {nome_pk} PRIMARY KEY ({coluna})")
        print(f"[OK] PK {nome_pk} em {tabela}")
    except Exception as e:
        print(f"[--] PK {nome_pk}: {str(e)[:110]}")

# COMMAND ----------

estrangeiras = [
    ("fato_geracao_horaria",    "sk_data",       "dim_data",          "sk_data"),
    ("fato_geracao_horaria",    "sk_hora",       "dim_hora",          "sk_hora"),
    ("fato_geracao_horaria",    "sk_subsistema", "dim_subsistema",    "sk_subsistema"),
    ("fato_geracao_horaria",    "sk_fonte",      "dim_fonte_geracao", "sk_fonte"),
    ("fato_balanco_horario",    "sk_data",       "dim_data",          "sk_data"),
    ("fato_balanco_horario",    "sk_hora",       "dim_hora",          "sk_hora"),
    ("fato_balanco_horario",    "sk_subsistema", "dim_subsistema",    "sk_subsistema"),
    ("fato_restricao_horaria",  "sk_data",       "dim_data",          "sk_data"),
    ("fato_restricao_horaria",  "sk_hora",       "dim_hora",          "sk_hora"),
    ("fato_restricao_horaria",  "sk_usina",      "dim_usina",         "sk_usina"),
    ("fato_restricao_horaria",  "sk_subsistema", "dim_subsistema",    "sk_subsistema"),
    ("fato_restricao_horaria",  "sk_fonte",      "dim_fonte_geracao", "sk_fonte"),
    ("fato_reservatorio_diario", "sk_data",      "dim_data",          "sk_data"),
    ("fato_reservatorio_diario", "sk_subsistema", "dim_subsistema",   "sk_subsistema"),
]

for fato, col_fato, dim, col_dim in estrangeiras:
    nome_fk = f"fk_{fato}_{col_fato}"
    try:
        spark.sql(f"ALTER TABLE {GOLD(fato)} ALTER COLUMN {col_fato} SET NOT NULL")
        spark.sql(
            f"ALTER TABLE {GOLD(fato)} ADD CONSTRAINT {nome_fk} "
            f"FOREIGN KEY ({col_fato}) REFERENCES {GOLD(dim)}({col_dim})"
        )
        print(f"[OK] FK {fato}.{col_fato} -> {dim}")
    except Exception as e:
        print(f"[--] FK {nome_fk}: {str(e)[:110]}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {GOLD()}"))

# COMMAND ----------

print("Camada Gold concluída. Próximo passo: notebook `04_qualidade_dados`.")
