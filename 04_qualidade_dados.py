# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Qualidade de dados
# MAGIC
# MAGIC **Etapa do MVP:** 4.5 (primeira parte)
# MAGIC
# MAGIC O enunciado pede a verificação de qualidade **no processo inicial de captura**. Por isso rodo
# MAGIC a perfilagem sobre a camada **Bronze**, onde o dado está como o ONS publicou. Rodar sobre o
# MAGIC Silver mediria a eficácia da minha própria limpeza, não a qualidade da fonte. As verificações
# MAGIC que dependem de tipo (acurácia, outliers) usam o Silver, porque não há como testar se uma
# MAGIC potência é negativa enquanto ela ainda é texto.
# MAGIC
# MAGIC Uso como **contrato de referência os dicionários de dados publicados pelo próprio ONS** — eles
# MAGIC dizem, campo a campo, se o valor pode ser nulo, zero ou negativo. Assim, cada achado abaixo é
# MAGIC confrontado com o que a fonte promete, e não com uma expectativa minha.
# MAGIC
# MAGIC | Dimensão | Como meço aqui |
# MAGIC |---|---|
# MAGIC | **Completude** | % de nulos e de sentinelas (`''`, `'-'`) por coluna, em todas as colunas da Bronze |
# MAGIC | **Unicidade** | Duplicatas na chave de negócio de cada tabela |
# MAGIC | **Consistência** | Perda no CAST, domínios categóricos, espaços de preenchimento, registros multilinha, identidades físicas |
# MAGIC | **Acurácia** | Plausibilidade física e aderência ao dicionário do ONS; validação do corte contra a GNRa oficial |
# MAGIC | **Outliers** | Método IQR (Tukey, 1,5×) sobre as métricas contínuas |
# MAGIC
# MAGIC **Persisto** o perfil de completude em `gold.qualidade_perfil_atributos`, para que a análise de
# MAGIC qualidade seja auditável e comparável entre execuções — e não um print perdido no notebook.
# MAGIC
# MAGIC > Os números citados nas células de texto são os da minha execução, com os dados coletados em
# MAGIC > 13/09/2026. As células de código recalculam tudo; se o ONS revisar os arquivos, os valores
# MAGIC > exibidos podem mudar ligeiramente.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

SENTINELAS = ["", "-", "--", "N/A", "NA", "null", "NULL"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude — perfilagem de todos os atributos da Bronze

# COMMAND ----------

def perfilar_completude(tabela: str) -> list:
    """Mede nulos, sentinelas e cardinalidade de cada coluna de uma tabela Bronze."""
    df = spark.table(tabela)
    colunas = [c for c in df.columns if not c.startswith("_")]
    total = df.count()

    agregacoes = []
    for c in colunas:
        agregacoes.append(F.sum(F.col(c).isNull().cast("long")).alias(f"{c}__nulo"))
        agregacoes.append(F.sum(F.trim(F.col(c)).isin(SENTINELAS).cast("long")).alias(f"{c}__sentinela"))
        agregacoes.append(F.approx_count_distinct(F.col(c)).alias(f"{c}__distintos"))

    resultado = df.agg(*agregacoes).collect()[0].asDict()

    linhas = []
    for c in colunas:
        nulos = resultado[f"{c}__nulo"] or 0
        sentinelas = resultado[f"{c}__sentinela"] or 0
        linhas.append(
            {
                "tabela": tabela.split(".")[-1],
                "coluna": c,
                "linhas_totais": total,
                "qtd_nulos": int(nulos),
                "qtd_sentinelas": int(sentinelas),
                "pct_ausente": round(100.0 * (nulos + sentinelas) / total, 4) if total else None,
                "qtd_valores_distintos": int(resultado[f"{c}__distintos"] or 0),
            }
        )
    return linhas


perfil = []
for chave in DATASETS.keys():
    print(f"Perfilando {chave}...")
    perfil.extend(perfilar_completude(BRONZE(chave)))

df_perfil = spark.createDataFrame(perfil).withColumn("_perfilado_em", F.current_timestamp())
df_perfil.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD("qualidade_perfil_atributos"))
spark.sql(
    f"COMMENT ON TABLE {GOLD('qualidade_perfil_atributos')} IS "
    "'Perfil de completude de todos os atributos das tabelas Bronze, medido no momento da "
    "captura. Base da análise de qualidade da etapa 4.5.'"
)

display(
    spark.table(GOLD("qualidade_perfil_atributos"))
    .filter(F.col("pct_ausente") > 0)
    .orderBy("tabela", F.desc("pct_ausente"))
    .select("tabela", "coluna", "linhas_totais", "qtd_nulos", "qtd_sentinelas", "pct_ausente")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como interpretei cada ausência — e o que fiz com ela
# MAGIC
# MAGIC Todas as colunas de balanço e de EAR estão **100% completas**. As ausências se concentram na
# MAGIC restrição e na capacidade, e **nenhuma delas é defeito da fonte**: cada uma tem um significado
# MAGIC que confirmei no dicionário do ONS. Tratar essas ausências como "dado faltante" e imputar valores
# MAGIC teria sido o erro mais grave possível neste pipeline.
# MAGIC
# MAGIC | Coluna | Ausência (eólica / solar) | O que significa | O que fiz |
# MAGIC |---|---|---|---|
# MAGIC | `cod_razaorestricao`, `cod_origemrestricao`, `val_geracaolimitada`, `val_geracaonaorealizadaapurada`, `num_minutos_*` | 59,9% / 72,5% | **Não houve restrição** naquela meia hora. O dicionário diz textualmente: *"se o campo for nulo, não houve limitação"* | Converti para `NULL` real e traduzi na coluna booleana `houve_restricao`. Preencher com valor padrão inventaria restrições que não existiram |
# MAGIC | `val_geracaoreferenciafinal` | 96,6% / 98,0% | É calculada **apenas nos períodos com razão REL** (valor enviado à CCEE) | Não uso como base do cálculo de corte; a seção 4.3 confirma que ela existe em 100% dos intervalos REL e em 0% dos demais |
# MAGIC | `dsc_restricao` | 74,1% / 81,2% | Campo criado pelo ONS em **26/09/2025** (dicionário v1.4). Antes de set/2025 está sempre vazio; desde então vem preenchido em todo intervalo restrito | Mantive no Silver e uso como evidência textual na análise da P5 |
# MAGIC | `ceg` (sentinela `-`) | 92,9% / 94,3% | O ONS usa `-` para **conjuntos de usinas**, que não têm CEG | Converti `-` para `NULL`; é por isso que o JOIN com a capacidade só casa usinas individuais (notebook 03) |
# MAGIC | `dat_desativacao` (capacidade) | 82,7% | Nulo significa **unidade geradora ativa** | Derivei a coluna `esta_ativa` |
# MAGIC | `dat_entradateste` (capacidade) | 0,14% | O dicionário permite nulo | Nenhum tratamento necessário |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência: `dsc_restricao` só existe a partir de setembro de 2025

# COMMAND ----------

display(spark.sql(f"""
    SELECT date_format(din_instante, 'yyyy-MM')                            AS mes,
           ROUND(100.0 * COUNT(dsc_restricao) / COUNT(*), 2)                 AS pct_com_descricao,
           ROUND(100.0 * COUNT(cod_razao_restricao) / COUNT(*), 2)           AS pct_com_restricao
    FROM {SILVER('restricao_coff_usina')}
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unicidade — duplicatas na chave de negócio

# COMMAND ----------

chaves_negocio = {
    "balanco_energia_subsistema": ["id_subsistema", "din_instante"],
    "ear_diario_subsistema": ["id_subsistema", "ear_data"],
    "restricao_coff_eolica": ["id_ons", "din_instante"],
    "restricao_coff_fotovoltaica": ["id_ons", "din_instante"],
    "capacidade_geracao": ["cod_equipamento", "nom_usina"],
}

resultados_unicidade = []
for tabela, chave in chaves_negocio.items():
    df = spark.table(BRONZE(tabela))
    total = df.count()
    distintos = df.select(*chave).distinct().count()
    resultados_unicidade.append(
        {
            "tabela": tabela,
            "chave_negocio": " + ".join(chave),
            "linhas": total,
            "chaves_distintas": distintos,
            "linhas_duplicadas": total - distintos,
            "pct_duplicado": round(100.0 * (total - distintos) / total, 4) if total else 0.0,
        }
    )

display(spark.createDataFrame(resultados_unicidade))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** não encontrei **nenhuma duplicata** em nenhuma das cinco tabelas. Mesmo assim,
# MAGIC mantenho o `dropDuplicates` na Silver. A razão é operacional: o download é reexecutável, e uma
# MAGIC reexecução parcial poderia reprocessar um arquivo já carregado. A deduplicação torna o pipeline
# MAGIC **idempotente** — rodar duas vezes produz o mesmo resultado que rodar uma.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consistência
# MAGIC
# MAGIC ### 3.1 Quantos valores se perdem no CAST?
# MAGIC
# MAGIC Com o modo ANSI desligado (ver `_config`), um valor que não converte vira `NULL` **em silêncio**.
# MAGIC Comparo os ausentes antes e depois da conversão: a diferença é a perda real.

# COMMAND ----------

def testar_conversao(tabela: str, coluna: str, tipo: str, formato: str = None):
    df = spark.table(BRONZE(tabela))
    limpo = F.trim(F.col(coluna))
    limpo = F.when(limpo.isin(SENTINELAS), F.lit(None)).otherwise(limpo)
    convertido = (
        F.to_timestamp(limpo, formato) if tipo == "timestamp"
        else F.to_date(limpo, formato) if tipo == "date"
        else F.regexp_replace(limpo, ",", "").cast(tipo)
    )
    r = df.select(
        F.sum(limpo.isNull().cast("long")).alias("ausente_antes"),
        F.sum(convertido.isNull().cast("long")).alias("nulo_depois"),
        F.count("*").alias("total"),
    ).collect()[0]
    perdidos = (r["nulo_depois"] or 0) - (r["ausente_antes"] or 0)
    return {
        "tabela": tabela, "coluna": coluna, "tipo_alvo": tipo,
        "ja_ausente": int(r["ausente_antes"] or 0), "perdidos_no_cast": int(perdidos),
        "pct_perdido": round(100.0 * perdidos / r["total"], 6) if r["total"] else 0.0,
        "status": "OK" if perdidos == 0 else "ATENÇÃO",
    }


testes = [
    ("balanco_energia_subsistema", "din_instante", "timestamp", "yyyy-MM-dd HH:mm:ss"),
    ("balanco_energia_subsistema", "val_carga", "double", None),
    ("balanco_energia_subsistema", "val_gereolica", "double", None),
    ("balanco_energia_subsistema", "val_gersolar", "double", None),
    ("balanco_energia_subsistema", "val_intercambio", "double", None),
    ("ear_diario_subsistema", "ear_data", "date", "yyyy-MM-dd"),
    ("ear_diario_subsistema", "ear_verif_subsistema_percentual", "double", None),
    ("restricao_coff_eolica", "din_instante", "timestamp", "yyyy-MM-dd HH:mm:ss"),
    ("restricao_coff_eolica", "val_geracao", "double", None),
    ("restricao_coff_eolica", "val_geracaoreferencia", "double", None),
    ("restricao_coff_eolica", "val_geracaonaorealizadaapurada", "double", None),
    ("restricao_coff_fotovoltaica", "din_instante", "timestamp", "yyyy-MM-dd HH:mm:ss"),
    ("restricao_coff_fotovoltaica", "val_geracao", "double", None),
    ("capacidade_geracao", "val_potenciaefetiva", "double", None),
    ("capacidade_geracao", "dat_entradaoperacao", "date", "yyyy-MM-dd"),
]

display(spark.createDataFrame([testar_conversao(*t) for t in testes]))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** **zero valores perdidos** em todas as conversões testadas. Todos os timestamps,
# MAGIC datas e números publicados pelo ONS estão em formato válido.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Os domínios categóricos são fechados?

# COMMAND ----------

for tabela, coluna in [
    ("restricao_coff_eolica", "cod_razaorestricao"),
    ("restricao_coff_eolica", "cod_origemrestricao"),
    ("restricao_coff_fotovoltaica", "cod_razaorestricao"),
    ("restricao_coff_fotovoltaica", "cod_origemrestricao"),
    ("balanco_energia_subsistema", "id_subsistema"),
    ("ear_diario_subsistema", "id_subsistema"),
    ("capacidade_geracao", "nom_tipousina"),
]:
    valores = (
        spark.table(BRONZE(tabela))
        .select(F.coalesce(F.trim(F.col(coluna)), F.lit("<nulo>")).alias("valor"))
        .groupBy("valor").count().orderBy(F.desc("count")).collect()
    )
    formatado = ", ".join(f"'{r['valor']}' ({r['count']:,})" for r in valores[:12])
    print(f"{tabela}.{coluna}\n   {len(valores)} valores: {formatado}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado e leitura:**
# MAGIC
# MAGIC - `cod_razaorestricao` só assume **REL, CNF e ENE**. O dicionário documenta também **PAR**
# MAGIC   (parecer de acesso), que não ocorreu no período — mesmo assim, meu mapeamento de rótulos no
# MAGIC   notebook 02 já o trata, para não quebrar quando aparecer.
# MAGIC - `cod_origemrestricao` só assume **SIS e LOC**, como documentado.
# MAGIC - O balanço traz **cinco** valores de `id_subsistema` (N, NE, SE, S e o agregado **SIN**). O EAR
# MAGIC   traz **apenas os quatro subsistemas reais** — a linha agregada existe só no balanço.
# MAGIC - A capacidade traz cinco tipos de usina, incluindo NUCLEAR, que não entra na minha análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Espaços de preenchimento

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'nom_subsistema' AS atributo, COUNT(*) AS linhas,
           SUM(CASE WHEN nom_subsistema <> TRIM(nom_subsistema) THEN 1 ELSE 0 END) AS com_espaco_extra,
           COUNT(DISTINCT nom_subsistema) AS distintos_sem_trim,
           COUNT(DISTINCT TRIM(nom_subsistema)) AS distintos_com_trim
    FROM {BRONZE('capacidade_geracao')}
    UNION ALL
    SELECT 'cod_equipamento', COUNT(*),
           SUM(CASE WHEN cod_equipamento <> TRIM(cod_equipamento) THEN 1 ELSE 0 END),
           COUNT(DISTINCT cod_equipamento), COUNT(DISTINCT TRIM(cod_equipamento))
    FROM {BRONZE('capacidade_geracao')}
    UNION ALL
    SELECT 'num_unidadegeradora', COUNT(*),
           SUM(CASE WHEN num_unidadegeradora <> TRIM(num_unidadegeradora) THEN 1 ELSE 0 END),
           COUNT(DISTINCT num_unidadegeradora), COUNT(DISTINCT TRIM(num_unidadegeradora))
    FROM {BRONZE('capacidade_geracao')}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado e leitura:** **100% das linhas** da capacidade trazem essas três colunas completadas
# MAGIC com espaços à direita — o arquivo foi gerado com largura fixa. Como o preenchimento é
# MAGIC **uniforme**, a contagem de distintos é igual com e sem `TRIM` *dentro* da tabela. O estrago
# MAGIC aparece quando o valor é comparado com **outra** tabela: `'NORDESTE       '` nunca é igual a
# MAGIC `'NORDESTE'`.
# MAGIC
# MAGIC *Tratamento:* apliquei `TRIM` a **todas** as colunas de texto no Silver, via a função
# MAGIC `texto_limpo()`, e não apenas nas colunas onde observei o problema — porque a fonte pode
# MAGIC introduzir o mesmo defeito em outra coluna na próxima publicação.
# MAGIC
# MAGIC Uma observação honesta: o `TRIM` era **necessário, mas não suficiente** para o JOIN da
# MAGIC `dim_usina`. Na primeira versão, mesmo com `TRIM`, o JOIN por nome casou **0 de 248** usinas —
# MAGIC por diferença de caixa e, principalmente, porque 233 das 248 entradas são conjuntos de usinas.
# MAGIC Troquei a chave do JOIN para o CEG (detalhes no notebook 03).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Registros com quebra de linha dentro de campo texto
# MAGIC
# MAGIC O ONS publica `dsc_restricao` e `nom_pontoconexao` entre aspas e, às vezes, com quebra de linha
# MAGIC dentro do texto. Na minha primeira leitura, sem `multiLine`, esses registros eram partidos em
# MAGIC fragmentos inválidos. A contagem abaixo mostra quantos registros teriam sido afetados.

# COMMAND ----------

for chave in ["restricao_coff_eolica", "restricao_coff_fotovoltaica"]:
    n = (
        spark.table(BRONZE(chave))
        .filter(F.col("dsc_restricao").contains("\n") | F.col("nom_pontoconexao").contains("\n"))
        .count()
    )
    print(f"{chave:<30} {n:>6,} registros com quebra de linha em campo texto")

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** 2.280 registros na eólica e 622 na solar. Com `multiLine=true` no notebook 01,
# MAGIC eles são lidos inteiros e **nenhum registro é descartado**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Identidades físicas do balanço
# MAGIC
# MAGIC Duas verificações que só fazem sentido para quem conhece o domínio:
# MAGIC
# MAGIC 1. **A linha SIN é a soma dos subsistemas?** Se for, confirmo que ela é um agregado e que somar
# MAGIC    tudo duplica a energia.
# MAGIC 2. **O intercâmbio fecha o balanço?** O dicionário do ONS não diz qual é o sinal do intercâmbio.
# MAGIC    Se `intercâmbio ≈ geração total − carga`, então positivo significa **exportação**.

# COMMAND ----------

display(spark.sql(f"""
    WITH sub AS (
        SELECT din_instante, SUM(carga_mwmed) AS soma_carga
        FROM {SILVER('balanco_subsistema_horario')} WHERE NOT eh_agregado GROUP BY din_instante
    )
    SELECT COUNT(*)                                                             AS horas_comparadas,
           ROUND(MAX(ABS(s.carga_mwmed - sub.soma_carga)), 3)                   AS maior_diferenca_mwmed,
           ROUND(SUM(s.carga_mwmed) / SUM(sub.soma_carga), 6)                    AS razao_sin_sobre_soma
    FROM {SILVER('balanco_subsistema_horario')} s JOIN sub USING (din_instante)
    WHERE s.eh_agregado
"""))

display(spark.sql(f"""
    SELECT id_subsistema,
           ROUND(corr(intercambio_mwmed, ger_total_mwmed - carga_mwmed), 4)                AS correlacao,
           ROUND(AVG(ABS(intercambio_mwmed - (ger_total_mwmed - carga_mwmed))), 1)          AS erro_medio_abs_mwmed,
           ROUND(AVG(ABS(intercambio_mwmed)), 1)                                           AS intercambio_medio_abs_mwmed
    FROM {SILVER('balanco_subsistema_horario')}
    WHERE NOT eh_agregado
    GROUP BY id_subsistema ORDER BY id_subsistema
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** a carga da linha SIN é **idêntica** à soma dos quatro subsistemas em todas as
# MAGIC 49.896 horas (diferença máxima zero). Somar tudo sem filtrar dá exatamente o dobro da carga.
# MAGIC
# MAGIC O intercâmbio tem correlação entre **0,99 e 1,00** com `geração total − carga` em todos os
# MAGIC subsistemas. Concluo que **intercâmbio positivo = exportação**, e é essa a convenção que uso na
# MAGIC análise da P7. O pequeno resíduo no Nordeste (erro médio de 24 MWmed contra um intercâmbio
# MAGIC médio de 5.257 MWmed) corresponde a perdas e arredondamentos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia
# MAGIC
# MAGIC ### 4.1 Os valores são fisicamente plausíveis e respeitam o dicionário?

# COMMAND ----------

display(spark.sql(f"""
    WITH b AS (SELECT * FROM {SILVER('balanco_subsistema_horario')} WHERE NOT eh_agregado),
         r AS (SELECT * FROM {SILVER('restricao_coff_usina')}),
         e AS (SELECT * FROM {SILVER('ear_subsistema_diario')} WHERE NOT eh_agregado)
    SELECT 'Carga negativa' AS teste, SUM(CASE WHEN carga_mwmed < 0 THEN 1 ELSE 0 END) AS violacoes, COUNT(*) AS avaliados FROM b
    UNION ALL SELECT 'Geração hidráulica negativa', SUM(CASE WHEN ger_hidraulica_mwmed < 0 THEN 1 ELSE 0 END), COUNT(*) FROM b
    UNION ALL SELECT 'Geração térmica negativa', SUM(CASE WHEN ger_termica_mwmed < 0 THEN 1 ELSE 0 END), COUNT(*) FROM b
    UNION ALL SELECT 'Geração eólica negativa', SUM(CASE WHEN ger_eolica_mwmed < 0 THEN 1 ELSE 0 END), COUNT(*) FROM b
    UNION ALL SELECT 'Geração solar negativa', SUM(CASE WHEN ger_solar_mwmed < 0 THEN 1 ELSE 0 END), COUNT(*) FROM b
    UNION ALL SELECT 'Geração solar > 1 MWmed entre 0h e 4h', SUM(CASE WHEN HOUR(din_instante) BETWEEN 0 AND 4 AND ger_solar_mwmed > 1 THEN 1 ELSE 0 END), COUNT(*) FROM b
    UNION ALL SELECT 'EAR fora de 0-100%', SUM(CASE WHEN ear_percentual < 0 OR ear_percentual > 100 THEN 1 ELSE 0 END), COUNT(*) FROM e
    UNION ALL SELECT 'Geração da usina > disponibilidade + 5%', SUM(CASE WHEN geracao_mwmed > disponibilidade_mwmed * 1.05 THEN 1 ELSE 0 END), COUNT(*) FROM r
    UNION ALL SELECT 'Geração negativa em usina', SUM(CASE WHEN geracao_mwmed < 0 THEN 1 ELSE 0 END), COUNT(*) FROM r
    UNION ALL SELECT 'Corte calculado negativo', SUM(CASE WHEN corte_mwh < 0 THEN 1 ELSE 0 END), COUNT(*) FROM r
    UNION ALL SELECT 'Restrição declarada sem energia cortada', SUM(CASE WHEN houve_restricao AND corte_mwh = 0 THEN 1 ELSE 0 END), COUNT(*) FROM r
    UNION ALL SELECT 'Restrição declarada sem geração de referência', SUM(CASE WHEN houve_restricao AND geracao_referencia_mwmed IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM r
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solar noturna: ruído ou erro?

# COMMAND ----------

display(spark.sql(f"""
    SELECT id_subsistema, YEAR(din_instante) AS ano, COUNT(*) AS horas,
           ROUND(AVG(ger_solar_mwmed), 1) AS media_mwmed, ROUND(MAX(ger_solar_mwmed), 1) AS maximo_mwmed
    FROM {SILVER('balanco_subsistema_horario')}
    WHERE NOT eh_agregado AND HOUR(din_instante) BETWEEN 0 AND 4 AND ger_solar_mwmed > 1
    GROUP BY 1, 2 ORDER BY 1, 2
"""))

display(spark.sql(f"""
    SELECT ROUND(100.0 * SUM(CASE WHEN HOUR(din_instante) BETWEEN 0 AND 4 AND ger_solar_mwmed > 1
                                  THEN ger_solar_mwmed ELSE 0 END) / SUM(ger_solar_mwmed), 4)
           AS pct_da_energia_solar_na_madrugada
    FROM {SILVER('balanco_subsistema_horario')} WHERE NOT eh_agregado
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Como leio os resultados de acurácia
# MAGIC
# MAGIC - **Nenhuma geração ou carga negativa no balanço** — o que bate com o dicionário, que não permite
# MAGIC   valor negativo nesses campos. Nenhum EAR fora de 0–100%.
# MAGIC - **Solar acima de 1 MWmed na madrugada (6.300 horas):** a maior parte são valores baixos (dezenas
# MAGIC   de MWmed), que leio como ruído de medição e consumo auxiliar. Há, porém, **picos isolados em
# MAGIC   2024 acima de 2.000 MWmed** no Nordeste, Sudeste e Sul, que só podem ser erro na fonte. *Não
# MAGIC   corrigi*, porque a energia envolvida é uma fração desprezível do total solar (célula acima) e
# MAGIC   nenhuma pergunta depende da geração solar noturna. Registro como limitação conhecida.
# MAGIC - **Geração acima da disponibilidade (1,8% dos registros):** o próprio dicionário explica — a
# MAGIC   geração inclui usinas **em operação em teste**, enquanto a disponibilidade considera só as em
# MAGIC   operação comercial. Não é erro.
# MAGIC - **Geração negativa em usina (21 registros, mínimo de −1,4 MWmed):** viola o dicionário, que não
# MAGIC   permite negativo. É consumo auxiliar registrado como geração. Mantive os valores: o
# MAGIC   `GREATEST(..., 0)` do cálculo de corte já neutraliza qualquer efeito.
# MAGIC - **Restrição declarada sem energia cortada (327.852 intervalos, 13,8% dos restritos):** a
# MAGIC   restrição existia, mas o recurso disponível já estava abaixo do limite imposto. É exatamente o
# MAGIC   caso que justifica o `GREATEST(..., 0)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Meu cálculo de corte bate com a GNRa oficial do ONS?
# MAGIC
# MAGIC Esta é a verificação de acurácia mais importante do MVP. Em 08/09/2026 o ONS passou a publicar a
# MAGIC **Geração Não Realizada Apurada (GNRa)** oficial. Se o meu `corte_mwh` — calculado de forma
# MAGIC independente — reproduzir a GNRa, toda a análise de P3 a P8 fica validada contra a fonte.

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                                                                          AS registros,
           ROUND(SUM(corte_mwh), 1)                                                          AS corte_calculado_mwh,
           ROUND(SUM(gnra_oficial_mwh), 1)                                                   AS gnra_oficial_mwh,
           ROUND(100.0 * (SUM(corte_mwh) - SUM(gnra_oficial_mwh)) / SUM(gnra_oficial_mwh), 4) AS diferenca_pct,
           ROUND(corr(corte_mwmed, gnra_oficial_mwmed), 6)                                    AS correlacao,
           SUM(CASE WHEN gnra_oficial_mwmed IS NOT NULL
                     AND ABS(corte_mwmed - gnra_oficial_mwmed) > 0.01 THEN 1 ELSE 0 END)     AS registros_divergentes,
           SUM(CASE WHEN gnra_oficial_mwmed IS NOT NULL AND NOT houve_restricao THEN 1 ELSE 0 END) AS gnra_sem_razao_declarada,
           SUM(CASE WHEN gnra_oficial_mwmed IS NULL AND houve_restricao THEN 1 ELSE 0 END)   AS razao_sem_gnra
    FROM {SILVER('restricao_coff_usina')}
"""))

display(spark.sql(f"""
    SELECT cod_fonte, date_format(din_instante, 'yyyy-MM') AS mes, COUNT(*) AS registros,
           ROUND(SUM(gnra_oficial_mwh), 1) AS gnra_mwh
    FROM {SILVER('restricao_coff_usina')}
    WHERE gnra_oficial_mwmed IS NOT NULL AND NOT houve_restricao
    GROUP BY 1, 2 ORDER BY 3 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** meu cálculo soma **61.438.603 MWh**; a GNRa oficial soma **61.439.589 MWh**. A
# MAGIC diferença é de **986 MWh (0,0016%)**, a correlação registro a registro é **0,99999** e só **28
# MAGIC de 2,37 milhões** de intervalos restritos divergem em mais de 0,01 MWmed.
# MAGIC
# MAGIC Consegui explicar a diferença inteira: são **42 registros de usinas solares** (maio e junho de
# MAGIC 2025 e fevereiro de 2026) em que o ONS apurou GNRa **sem declarar razão de restrição**. Como a
# MAGIC minha regra só conta corte quando há razão declarada, esses 986 MWh ficam de fora. A soma da GNRa
# MAGIC desses 42 registros é exatamente a diferença.
# MAGIC
# MAGIC Com isso, deixo de tratar o corte como uma *estimativa minha* e passo a tratá-lo como um valor
# MAGIC **validado contra a apuração oficial**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 `val_geracaoreferenciafinal` existe só nos intervalos REL?

# COMMAND ----------

display(spark.sql(f"""
    SELECT COALESCE(cod_razao_restricao, '<sem restrição>') AS razao,
           COUNT(*) AS registros,
           SUM(CASE WHEN geracao_referencia_final_mwmed IS NOT NULL THEN 1 ELSE 0 END) AS com_referencia_final,
           ROUND(100.0 * SUM(CASE WHEN geracao_referencia_final_mwmed IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct
    FROM {SILVER('restricao_coff_usina')}
    GROUP BY 1 ORDER BY 2 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** a coluna está preenchida em **100% dos intervalos REL** (193.333) e em **0%** de
# MAGIC todos os outros. Confirma o dicionário: a ausência de 96–98% não é falha, é regra de apuração.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers — método IQR sobre as métricas contínuas

# COMMAND ----------

def detectar_outliers(tabela: str, coluna: str, rotulo: str, filtro: str = "1=1"):
    """Critério de Tukey: fora de [Q1 − 1,5·IQR ; Q3 + 1,5·IQR]."""
    df = spark.table(tabela).filter(filtro).filter(F.col(coluna).isNotNull())
    q1, med, q3 = df.approxQuantile(coluna, [0.25, 0.5, 0.75], 0.001)
    iqr = q3 - q1
    inferior, superior = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    r = df.select(
        F.count("*").alias("n"), F.min(coluna).alias("minimo"), F.max(coluna).alias("maximo"),
        F.sum(((F.col(coluna) < inferior) | (F.col(coluna) > superior)).cast("long")).alias("out"),
    ).collect()[0]
    return {
        "metrica": rotulo, "n": r["n"], "minimo": round(r["minimo"], 3), "q1": round(q1, 3),
        "mediana": round(med, 3), "q3": round(q3, 3), "maximo": round(r["maximo"], 3),
        "limite_inferior": round(inferior, 3), "limite_superior": round(superior, 3),
        "qtd_outliers": int(r["out"] or 0),
        "pct_outliers": round(100.0 * (r["out"] or 0) / r["n"], 3) if r["n"] else 0.0,
    }


analises_outlier = [
    (SILVER("balanco_subsistema_horario"), "carga_mwmed", "Carga do subsistema (MWmed)", "NOT eh_agregado"),
    (SILVER("balanco_subsistema_horario"), "ger_eolica_mwmed", "Geração eólica (MWmed)", "NOT eh_agregado"),
    (SILVER("balanco_subsistema_horario"), "ger_solar_mwmed", "Geração solar (MWmed)", "NOT eh_agregado"),
    (SILVER("balanco_subsistema_horario"), "intercambio_mwmed", "Intercâmbio (MWmed)", "NOT eh_agregado"),
    (SILVER("ear_subsistema_diario"), "ear_percentual", "EAR (%)", "NOT eh_agregado"),
    (SILVER("restricao_coff_usina"), "corte_mwh", "Corte por usina / 30 min (MWh)", "houve_restricao"),
]

display(spark.createDataFrame([detectar_outliers(*a) for a in analises_outlier]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por que não removi nenhum outlier
# MAGIC
# MAGIC Na minha execução o IQR apontou **7,5%** de outliers na carga, **22,5%** na geração eólica,
# MAGIC **14,4%** na solar, **8,9%** no corte e **zero** no intercâmbio e no EAR. **Não removi nenhum, e
# MAGIC isso é uma decisão deliberada minha**, não omissão:
# MAGIC
# MAGIC - **Carga e geração por subsistema** misturam, na mesma coluna, subsistemas de portes muito
# MAGIC   diferentes (o Sudeste tem várias vezes a carga do Norte). Os "outliers" são simplesmente o
# MAGIC   Sudeste. Remover apagaria um subsistema inteiro.
# MAGIC - **Geração solar** é bimodal por natureza — zero à noite, alta ao meio-dia. O IQR pressupõe uma
# MAGIC   distribuição aproximadamente unimodal e classifica os picos diurnos legítimos como extremos.
# MAGIC - **Corte por constrained-off** é um fenômeno de cauda: o que interessa investigar são justamente
# MAGIC   os eventos extremos. Remover os outliers apagaria o objeto de estudo do MVP.
# MAGIC
# MAGIC Uso a perfilagem para **conhecer a distribuição e escolher a estatística certa** — mediana onde a
# MAGIC cauda pesa, agregação por subsistema e por período em vez de comparação entre valores isolados.
# MAGIC Os únicos valores que considero erro de fato (solar noturna acima de 2 GW em 2024) estão
# MAGIC documentados na seção 4.1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cobertura temporal — há lacunas nas séries?

# COMMAND ----------

display(spark.sql(f"""
    SELECT YEAR(din_instante) AS ano,
           COUNT(DISTINCT DATE(din_instante)) AS dias_com_dado,
           COUNT(*) AS registros,
           COUNT(DISTINCT id_subsistema) AS subsistemas,
           ROUND(COUNT(*) / (COUNT(DISTINCT DATE(din_instante)) * COUNT(DISTINCT id_subsistema)), 2) AS registros_por_dia_subsistema
    FROM {SILVER('balanco_subsistema_horario')}
    WHERE NOT eh_agregado
    GROUP BY 1 ORDER BY 1
"""))

display(spark.sql(f"""
    SELECT date_format(din_instante, 'yyyy-MM') AS mes,
           COUNT(DISTINCT id_ons) AS usinas,
           COUNT(*) AS registros,
           ROUND(COUNT(*) / COUNT(DISTINCT id_ons), 1) AS registros_por_usina
    FROM {SILVER('restricao_coff_usina')}
    GROUP BY 1 ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Resultado:** o balanço tem exatamente **24 registros por dia por subsistema em todos os anos**
# MAGIC — nenhuma lacuna. O ano de 2026 tem 253 dias porque o ONS publica até a data mais recente
# MAGIC (10/09/2026 na minha coleta). Na restrição, cada usina tem entre ~1.340 e ~1.490 registros por mês,
# MAGIC o que corresponde a 48 intervalos por dia vezes os dias do mês; a quantidade de usinas monitoradas
# MAGIC cresce de 221 para 236 ao longo do período, conforme novas usinas entram em operação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Síntese da qualidade

# COMMAND ----------

display(spark.sql(f"""
    SELECT tabela,
           COUNT(*) AS atributos,
           SUM(CASE WHEN pct_ausente = 0 THEN 1 ELSE 0 END) AS completos,
           SUM(CASE WHEN pct_ausente > 0 AND pct_ausente < 50 THEN 1 ELSE 0 END) AS ausencia_parcial,
           SUM(CASE WHEN pct_ausente >= 50 THEN 1 ELSE 0 END) AS ausencia_estrutural,
           MAX(linhas_totais) AS linhas
    FROM {GOLD('qualidade_perfil_atributos')}
    GROUP BY tabela ORDER BY tabela
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da etapa de qualidade
# MAGIC
# MAGIC Os dados do ONS são **tecnicamente limpos**: zero duplicatas, zero perdas de conversão, zero
# MAGIC lacunas temporais e um cálculo de corte que reproduz a apuração oficial com diferença de 0,0016%.
# MAGIC
# MAGIC Os riscos reais não estavam em "sujeira", e sim em **semântica**: uma linha agregada que dobra os
# MAGIC totais, ausências que significam "não houve restrição", uma coluna que só existe para um tipo de
# MAGIC razão, um campo que só passou a existir em setembro de 2025, conjuntos de usinas que não têm CEG e
# MAGIC registros multilinha. Nenhum desses problemas geraria erro de execução — todos gerariam números
# MAGIC errados e plausíveis. Foi a leitura do dicionário de dados da fonte, e não a perfilagem sozinha,
# MAGIC que me permitiu tratá-los corretamente.

# COMMAND ----------

print("Análise de qualidade concluída. Próximo passo: notebook `05_analise_perguntas`.")
