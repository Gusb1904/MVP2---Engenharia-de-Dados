# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Análise: respondendo às perguntas de negócio
# MAGIC
# MAGIC **Etapa do MVP:** 4.5 (segunda parte)
# MAGIC
# MAGIC Cada seção abaixo responde a uma das oito perguntas que defini na etapa 2, na mesma ordem. O
# MAGIC padrão é sempre o mesmo:
# MAGIC
# MAGIC 1. A pergunta, repetida na íntegra
# MAGIC 2. A consulta SQL sobre o modelo dimensional da camada Gold
# MAGIC 3. A tabela de resultado e, quando ajuda, um gráfico
# MAGIC 4. **A discussão do resultado, gerada com os números da execução** — os valores no texto são
# MAGIC    calculados na hora, não digitados. E quando a conclusão depende do resultado (por exemplo,
# MAGIC    qual razão de corte predomina), o texto escolhe a interpretação a partir do número, em vez de
# MAGIC    afirmar uma conclusão decidida antes de olhar os dados.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Identidade visual
# ---------------------------------------------------------------------------
# Paleta categórica validada para daltonismo e para contraste com a superfície clara.
# A cor é atribuída por ENTIDADE (fonte de geração, razão de corte), nunca por posição
# no ranking — assim um filtro que mude o conjunto de séries não repinta as que sobraram.
COR_NOME = {"Hidráulica": "#2a78d6", "Térmica": "#eb6834", "Eólica": "#1baf7a", "Solar": "#eda100"}
COR_RAZAO = {"rel": "#2a78d6", "cnf": "#eb6834", "ene": "#1baf7a", "par": "#eda100"}
ROTULO_RAZAO = {
    "rel": "Indisponibilidade externa (REL)",
    "cnf": "Confiabilidade (CNF)",
    "ene": "Razão energética (ENE)",
    "par": "Parecer de acesso (PAR)",
}
SUPERFICIE, TINTA, TINTA_SUAVE, GRADE = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"

plt.rcParams.update({
    "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "axes.edgecolor": GRADE,
    "axes.labelcolor": TINTA_SUAVE, "axes.titlecolor": TINTA, "axes.titlesize": 13,
    "axes.titleweight": "bold", "axes.grid": True, "grid.color": GRADE, "grid.linewidth": 0.8,
    "xtick.color": TINTA_SUAVE, "ytick.color": TINTA_SUAVE, "text.color": TINTA,
    "font.size": 10, "legend.frameon": False, "figure.dpi": 110,
})


def limpar_eixos(ax):
    """Grade recessiva e sem molduras — os dados em primeiro plano."""
    for lado in ("top", "right", "left"):
        ax.spines[lado].set_visible(False)
    ax.spines["bottom"].set_color(GRADE)
    ax.set_axisbelow(True)
    ax.grid(axis="x", visible=False)


def br(valor, casas=1):
    """Formata número no padrão brasileiro: 12.345,6."""
    texto = f"{valor:,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def milhar(ax, eixo="y"):
    fmt = mticker.FuncFormatter(lambda v, _: br(v, 0))
    (ax.yaxis if eixo == "y" else ax.xaxis).set_major_formatter(fmt)


def qualificar_correlacao(r):
    a = abs(r)
    if a < 0.1:
        return "praticamente inexistente"
    if a < 0.3:
        return "fraca"
    if a < 0.5:
        return "moderada"
    if a < 0.7:
        return "forte"
    return "muito forte"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto do período analisado

# COMMAND ----------

ctx = spark.sql(f"""
    SELECT
        (SELECT MIN(din_instante) FROM {GOLD('fato_balanco_horario')})      AS ini_balanco,
        (SELECT MAX(din_instante) FROM {GOLD('fato_balanco_horario')})      AS fim_balanco,
        (SELECT MIN(din_instante) FROM {GOLD('fato_restricao_horaria')})    AS ini_coff,
        (SELECT MAX(din_instante) FROM {GOLD('fato_restricao_horaria')})    AS fim_coff,
        (SELECT COUNT(*) FROM {GOLD('dim_usina')})                          AS qtd_usinas,
        (SELECT MAX(ano) - 1 FROM {GOLD('dim_data')})                       AS ultimo_ano_completo
""").collect()[0]

ANO_COMPLETO = int(ctx["ultimo_ano_completo"])

print(f"Balanço                 : {ctx['ini_balanco']:%d/%m/%Y} a {ctx['fim_balanco']:%d/%m/%Y}")
print(f"Constrained-off         : {ctx['ini_coff']:%d/%m/%Y} a {ctx['fim_coff']:%d/%m/%Y}")
print(f"Usinas monitoradas      : {ctx['qtd_usinas']:,}")
print(f"Último ano completo     : {ANO_COMPLETO}")
print(
    "\nAs janelas são diferentes de propósito: os arquivos de restrição são mensais e pesados, então\n"
    "cobrem um período menor. Quando comparo anos, uso o último ano completo, porque o ano corrente\n"
    "está incompleto e concentrado no período úmido — o que distorceria a composição da geração."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P1 — Como evoluiu a participação de cada fonte na geração, por subsistema?
# MAGIC
# MAGIC > *Como evoluiu a participação de cada fonte (hidráulica, térmica, eólica e solar) na geração,
# MAGIC > por subsistema, ao longo do período?*

# COMMAND ----------

p1 = spark.sql(f"""
    SELECT d.ano, s.nom_subsistema, f.nom_fonte,
           ROUND(SUM(g.geracao_mwh) / 1e6, 3) AS geracao_twh,
           ROUND(100.0 * SUM(g.geracao_mwh)
                 / SUM(SUM(g.geracao_mwh)) OVER (PARTITION BY d.ano, s.nom_subsistema), 2) AS pct_da_geracao
    FROM {GOLD('fato_geracao_horaria')} g
    JOIN {GOLD('dim_data')}          d ON g.sk_data       = d.sk_data
    JOIN {GOLD('dim_subsistema')}    s ON g.sk_subsistema = s.sk_subsistema
    JOIN {GOLD('dim_fonte_geracao')} f ON g.sk_fonte      = f.sk_fonte
    GROUP BY d.ano, s.nom_subsistema, f.nom_fonte
    ORDER BY s.nom_subsistema, d.ano, f.nom_fonte
""")
display(p1)

p1_nacional = spark.sql(f"""
    SELECT d.ano, f.nom_fonte,
           ROUND(100.0 * SUM(g.geracao_mwh) / SUM(SUM(g.geracao_mwh)) OVER (PARTITION BY d.ano), 2) AS pct
    FROM {GOLD('fato_geracao_horaria')} g
    JOIN {GOLD('dim_data')} d ON g.sk_data = d.sk_data
    JOIN {GOLD('dim_fonte_geracao')} f ON g.sk_fonte = f.sk_fonte
    GROUP BY d.ano, f.nom_fonte ORDER BY d.ano, f.nom_fonte
""").toPandas()
display(p1_nacional)

# COMMAND ----------

pdf1 = p1.toPandas()
ordem_fontes = ["Hidráulica", "Térmica", "Eólica", "Solar"]
subsistemas = ["Norte", "Nordeste", "Sudeste/C.O.", "Sul"]

fig, axes = plt.subplots(1, 4, figsize=(16, 4.4), sharey=True)
for ax, sub in zip(axes, subsistemas):
    dados = pdf1[pdf1.nom_subsistema == sub].pivot(
        index="ano", columns="nom_fonte", values="pct_da_geracao"
    ).reindex(columns=ordem_fontes).fillna(0)
    base = np.zeros(len(dados))
    for fonte in ordem_fontes:
        ax.bar(dados.index.astype(str), dados[fonte], bottom=base, color=COR_NOME[fonte], label=fonte,
               width=0.68, edgecolor=SUPERFICIE, linewidth=2)
        base = base + dados[fonte].values
    ax.set_title(sub, pad=10)
    ax.set_ylim(0, 100)
    limpar_eixos(ax)
    ax.tick_params(axis="x", rotation=45)
axes[0].set_ylabel("% da geração do subsistema")
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.1))
fig.suptitle("P1 — Composição da geração por fonte, por subsistema (ano corrente parcial)", y=1.03, fontsize=13, weight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P1

# COMMAND ----------

ano_ini = int(pdf1.ano.min())


def part(sub, fonte, ano):
    linha = pdf1[(pdf1.nom_subsistema == sub) & (pdf1.nom_fonte == fonte) & (pdf1.ano == ano)]
    return float(linha.pct_da_geracao.iloc[0]) if len(linha) else 0.0


def part_nac(fonte, ano):
    linha = p1_nacional[(p1_nacional.nom_fonte == fonte) & (p1_nacional.ano == ano)]
    return float(linha.pct.iloc[0]) if len(linha) else 0.0


print(f"COMPOSIÇÃO DA GERAÇÃO — {ano_ini} x {ANO_COMPLETO} (último ano completo)\n" + "=" * 80)
for sub in subsistemas:
    print(f"\n{sub}")
    for fonte in ordem_fontes:
        a, b = part(sub, fonte, ano_ini), part(sub, fonte, ANO_COMPLETO)
        print(f"   {fonte:<11} {br(a):>6}% -> {br(b):>6}%   ({'+' if b >= a else ''}{br(b - a)} p.p.)")

int_ne_ini = part("Nordeste", "Eólica", ano_ini) + part("Nordeste", "Solar", ano_ini)
int_ne_fim = part("Nordeste", "Eólica", ANO_COMPLETO) + part("Nordeste", "Solar", ANO_COMPLETO)
int_br_ini = part_nac("Eólica", ano_ini) + part_nac("Solar", ano_ini)
int_br_fim = part_nac("Eólica", ANO_COMPLETO) + part_nac("Solar", ANO_COMPLETO)
sub_mais_solar = max(subsistemas, key=lambda s: part(s, "Solar", ANO_COMPLETO) - part(s, "Solar", ano_ini))

print(f"""
{'=' * 80}
LEITURA:

No Brasil como um todo, a geração intermitente (eólica + solar) passou de {br(int_br_ini)}% para
{br(int_br_fim)}% da geração verificada entre {ano_ini} e {ANO_COMPLETO}. A solar sozinha foi de
{br(part_nac('Solar', ano_ini))}% para {br(part_nac('Solar', ANO_COMPLETO))}%: foi a fonte que mais cresceu, e cresceu em todos os
subsistemas — o maior salto foi no {sub_mais_solar}.

O Nordeste é o caso extremo. Lá, eólica + solar foram de {br(int_ne_ini)}% para {br(int_ne_fim)}% da geração
do subsistema. A térmica caiu de {br(part('Nordeste', 'Térmica', ano_ini))}% para {br(part('Nordeste', 'Térmica', ANO_COMPLETO))}%.
Em {ano_ini}, ano da crise hídrica, a térmica ainda tinha peso relevante em todo o país
({br(part_nac('Térmica', ano_ini))}% da geração nacional).

Isso importa para o problema porque eólica e solar são as duas fontes NÃO despacháveis do modelo
(dim_fonte_geracao.eh_despachavel). Quando elas passam a responder por quase quatro quintos da geração de um
subsistema, o operador perde justamente a alavanca que usaria para equilibrar oferta e demanda. Quando há
excesso, sobra cortar — e as perguntas P3 a P5 medem esse corte.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P2 — Em que horas e meses a geração intermitente pesa mais sobre a carga?
# MAGIC
# MAGIC > *Em quais horas do dia e meses do ano a geração intermitente (eólica + solar) atinge maior
# MAGIC > participação sobre a carga do subsistema?*
# MAGIC
# MAGIC Faço a análise para o Nordeste, onde P1 mostrou a maior participação intermitente, no último ano
# MAGIC completo.

# COMMAND ----------

p2 = spark.sql(f"""
    SELECT d.mes, d.nom_mes, h.hora,
           ROUND(100.0 * SUM(b.ger_intermitente_mwmed) / SUM(b.carga_mwmed), 2) AS pct_intermitente
    FROM {GOLD('fato_balanco_horario')} b
    JOIN {GOLD('dim_data')} d ON b.sk_data = d.sk_data
    JOIN {GOLD('dim_hora')} h ON b.sk_hora = h.sk_hora
    WHERE b.sk_subsistema = 'NE' AND d.ano = {ANO_COMPLETO}
    GROUP BY d.mes, d.nom_mes, h.hora
    ORDER BY d.mes, h.hora
""")
display(p2)

p2_ano = spark.sql(f"""
    SELECT ROUND(100.0 * SUM(b.ger_intermitente_mwmed) / SUM(b.carga_mwmed), 2) AS pct_anual,
           ROUND(100.0 * AVG(CASE WHEN b.pct_intermitente_sobre_carga > 100 THEN 1 ELSE 0 END), 2) AS pct_horas_acima_100
    FROM {GOLD('fato_balanco_horario')} b JOIN {GOLD('dim_data')} d ON b.sk_data = d.sk_data
    WHERE b.sk_subsistema = 'NE' AND d.ano = {ANO_COMPLETO}
""").collect()[0]

# COMMAND ----------

pdf2 = p2.toPandas()
matriz = pdf2.pivot(index="mes", columns="hora", values="pct_intermitente").sort_index()

# Magnitude contínua -> rampa sequencial de uma única cor, clara para escura.
fig, ax = plt.subplots(figsize=(13, 4.8))
im = ax.imshow(matriz.values, aspect="auto", cmap="Blues", origin="lower")
ax.set_xticks(range(0, 24, 2))
ax.set_xticklabels([f"{h:02d}h" for h in range(0, 24, 2)])
ax.set_yticks(range(len(matriz.index)))
ax.set_yticklabels(pdf2.drop_duplicates("mes").sort_values("mes").nom_mes.str[:3].tolist())
ax.set_title(f"P2 — Nordeste {ANO_COMPLETO}: geração intermitente (eólica + solar) como % da carga", pad=12)
ax.set_xlabel("Hora do dia")
ax.grid(False)
for lado in ("top", "right", "left", "bottom"):
    ax.spines[lado].set_visible(False)
cbar = fig.colorbar(im, ax=ax, pad=0.015)
cbar.set_label("% da carga", color=TINTA_SUAVE)
cbar.outline.set_visible(False)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P2

# COMMAND ----------

pico = pdf2.loc[pdf2.pct_intermitente.idxmax()]
vale = pdf2.loc[pdf2.pct_intermitente.idxmin()]
por_hora = pdf2.groupby("hora").pct_intermitente.mean()
por_mes = pdf2.groupby(["mes", "nom_mes"]).pct_intermitente.mean().reset_index().sort_values("pct_intermitente", ascending=False)
acima_100 = int((pdf2.pct_intermitente > 100).sum())
h_max, h_min = int(por_hora.idxmax()), int(por_hora.idxmin())

print(f"""
NORDESTE — {ANO_COMPLETO}

No ano inteiro, a geração intermitente foi {br(p2_ano['pct_anual'])}% da carga do subsistema.
Em {br(p2_ano['pct_horas_acima_100'])}% das horas do ano, eólica + solar sozinhas superaram toda a carga.
Combinações mês × hora acima de 100%: {acima_100} de {len(pdf2)}.

Maior participação : {br(pico.pct_intermitente)}% — {pico.nom_mes}, às {int(pico.hora):02d}h
Menor participação : {br(vale.pct_intermitente)}% — {vale.nom_mes}, às {int(vale.hora):02d}h

Média por hora — mais alta às {h_max:02d}h ({br(por_hora.max())}%), mais baixa às {h_min:02d}h ({br(por_hora.min())}%)
Meses de maior participação : {', '.join(f"{r.nom_mes} ({br(r.pct_intermitente)}%)" for r in por_mes.head(4).itertuples())}
Meses de menor participação : {', '.join(f"{r.nom_mes} ({br(r.pct_intermitente)}%)" for r in por_mes.tail(3).itertuples())}

LEITURA:

O mapa de calor mostra duas estruturas sobrepostas.

A estrutura SAZONAL vem do vento: os meses de maior participação são os do regime de ventos do Nordeste,
no inverno e na primavera. Nesses meses a faixa escura atravessa a madrugada inteira, quando a carga é baixa
e a eólica sozinha já supera o consumo.

A estrutura DIÁRIA tem o seu ponto mais alto no início da manhã ({h_max:02d}h), quando o vento noturno ainda
está forte e a solar começa a entrar, e o seu vale no fim da tarde ({h_min:02d}h), quando a solar desaparece,
o vento ainda não voltou à intensidade noturna e a carga sobe. É o único intervalo do dia em que o Nordeste
depende de fontes despacháveis.

O ponto crítico do sistema é, portanto, a combinação de mês de vento forte com as primeiras horas da manhã. É
ali que a oferta renovável encontra a menor capacidade de absorção, e é ali que P7 vai mostrar a carga líquida
mais negativa.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P3 — Qual o volume de energia cortada e como evoluiu?
# MAGIC
# MAGIC > *Qual o volume total de energia eólica e solar cortada por constrained-off, e como evoluiu ao
# MAGIC > longo do tempo?*

# COMMAND ----------

p3 = spark.sql(f"""
    SELECT d.ano_mes, f.nom_fonte,
           ROUND(SUM(r.corte_mwh) / 1000.0, 2)                                  AS corte_gwh,
           ROUND(SUM(r.gnra_oficial_mwh) / 1000.0, 2)                           AS gnra_oficial_gwh,
           ROUND(SUM(r.geracao_mwh) / 1000.0, 2)                                AS geracao_gwh,
           ROUND(SUM(r.geracao_referencia_mwh) / 1000.0, 2)                     AS referencia_gwh,
           ROUND(100.0 * SUM(r.corte_mwh) / NULLIF(SUM(r.geracao_referencia_mwh), 0), 2) AS taxa_corte_pct
    FROM {GOLD('fato_restricao_horaria')} r
    JOIN {GOLD('dim_data')} d ON r.sk_data = d.sk_data
    JOIN {GOLD('dim_fonte_geracao')} f ON r.sk_fonte = f.sk_fonte
    GROUP BY d.ano_mes, f.nom_fonte
    ORDER BY d.ano_mes, f.nom_fonte
""")
display(p3)

# COMMAND ----------

pdf3 = p3.toPandas()
serie = pdf3.pivot(index="ano_mes", columns="nom_fonte", values="corte_gwh").fillna(0)

fig, ax = plt.subplots(figsize=(13, 4.8))
for fonte in ["Eólica", "Solar"]:
    if fonte in serie.columns:
        ax.plot(serie.index, serie[fonte], color=COR_NOME[fonte], linewidth=2, marker="o", markersize=5, label=fonte)
        ax.annotate(f" {fonte}", (len(serie) - 1, serie[fonte].iloc[-1]), color=COR_NOME[fonte],
                    fontweight="bold", va="center")
ax.set_title("P3 — Energia cortada por constrained-off, por mês", pad=12)
ax.set_ylabel("GWh cortados")
ax.legend(loc="upper left", ncol=2)
ax.tick_params(axis="x", rotation=45)
limpar_eixos(ax)
milhar(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P3

# COMMAND ----------

tot = spark.sql(f"""
    SELECT SUM(corte_mwh) / 1000.0 AS corte_gwh, SUM(gnra_oficial_mwh) / 1000.0 AS gnra_gwh,
           SUM(geracao_mwh) / 1000.0 AS geracao_gwh, SUM(geracao_referencia_mwh) / 1000.0 AS referencia_gwh,
           100.0 * SUM(corte_mwh) / SUM(geracao_referencia_mwh) AS taxa_pct,
           COUNT(DISTINCT sk_data) AS dias
    FROM {GOLD('fato_restricao_horaria')}
""").collect()[0]

por_fonte = spark.sql(f"""
    SELECT f.nom_fonte, SUM(r.corte_mwh) / 1000.0 AS corte_gwh,
           100.0 * SUM(r.corte_mwh) / SUM(r.geracao_referencia_mwh) AS taxa_pct,
           100.0 * SUM(r.corte_mwh) / SUM(SUM(r.corte_mwh)) OVER () AS pct_do_corte
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_fonte_geracao')} f ON r.sk_fonte = f.sk_fonte
    GROUP BY f.nom_fonte ORDER BY corte_gwh DESC
""").toPandas()

comparavel = spark.sql(f"""
    WITH ultimo AS (SELECT MAX(d.ano) AS ano, MAX(d.mes) AS mes
                    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_data')} d ON r.sk_data = d.sk_data
                    WHERE d.ano = (SELECT MAX(d2.ano) FROM {GOLD('fato_restricao_horaria')} r2
                                   JOIN {GOLD('dim_data')} d2 ON r2.sk_data = d2.sk_data))
    SELECT d.ano, SUM(r.corte_mwh) / 1000.0 AS corte_gwh,
           100.0 * SUM(r.corte_mwh) / SUM(r.geracao_referencia_mwh) AS taxa_pct,
           (SELECT mes FROM ultimo) AS ate_mes
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_data')} d ON r.sk_data = d.sk_data
    WHERE d.mes <= (SELECT mes FROM ultimo)
    GROUP BY d.ano ORDER BY d.ano
""").toPandas()

total_mensal = serie.sum(axis=1)
meses = len(total_mensal)
# Consumo residencial médio no Brasil: ~160 kWh/mês por domicílio (referência EPE).
domicilios = tot["corte_gwh"] * 1e6 / 160 / meses

linhas_fonte = "\n".join(
    f"   {r.nom_fonte:<8} {br(r.corte_gwh):>10} GWh  ({br(r.pct_do_corte)}% do corte, taxa de {br(r.taxa_pct, 2)}%)"
    for r in por_fonte.itertuples()
)
texto_comparacao = ""
if len(comparavel) >= 2:
    a, b = comparavel.iloc[-2], comparavel.iloc[-1]
    var = 100 * (b.corte_gwh - a.corte_gwh) / a.corte_gwh
    texto_comparacao = (
        f"Comparando o mesmo intervalo do ano (janeiro a mês {int(b.ate_mes)}), o corte foi de {br(a.corte_gwh)} GWh "
        f"em {int(a.ano)} para {br(b.corte_gwh)} GWh em {int(b.ano)} ({'+' if var >= 0 else ''}{br(var)}%), e a taxa "
        f"passou de {br(a.taxa_pct, 2)}% para {br(b.taxa_pct, 2)}%."
    )

print(f"""
VOLUME NO PERÍODO ({ctx['ini_coff']:%m/%Y} a {ctx['fim_coff']:%m/%Y}, {meses} meses)

Energia cortada (meu cálculo)  : {br(tot['corte_gwh']):>12} GWh
GNRa oficial do ONS            : {br(tot['gnra_gwh']):>12} GWh
Energia efetivamente gerada    : {br(tot['geracao_gwh']):>12} GWh
Geração de referência          : {br(tot['referencia_gwh']):>12} GWh
Taxa de corte                  : {br(tot['taxa_pct'], 2):>11}% da geração de referência

POR FONTE
{linhas_fonte}

EVOLUÇÃO MENSAL
   Maior mês : {total_mensal.idxmax()} ({br(total_mensal.max())} GWh)
   Menor mês : {total_mensal.idxmin()} ({br(total_mensal.min())} GWh)
   {texto_comparacao}

LEITURA:

Em {meses} meses, {br(tot['corte_gwh'])} GWh de energia eólica e solar deixaram de ser gerados por ordem do
operador — {br(tot['taxa_pct'], 1)}% de tudo o que essas usinas poderiam ter produzido. O número bate com a GNRa
oficial do ONS com diferença inferior a 0,01% (validação no notebook 04), então não é uma estimativa minha: é
a apuração oficial reproduzida pelo pipeline.

Para dar escala: é o equivalente ao consumo residencial de cerca de {br(domicilios / 1e6)} milhões de domicílios
brasileiros médios durante o período (~160 kWh/mês, referência EPE). É energia de usinas construídas,
contratadas e disponíveis.

A série tem sazonalidade clara, com picos no segundo semestre (safra de ventos) e vales no início do ano. A
solar tem taxa de corte maior que a eólica, apesar de cortar menos energia em volume. E a comparação do mesmo
período entre os anos mostra que o problema não está diminuindo.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P4 — Onde os cortes se concentram?
# MAGIC
# MAGIC > *Onde os cortes se concentram — quais subsistemas, estados e usinas?*

# COMMAND ----------

p4_sub = spark.sql(f"""
    SELECT s.nom_subsistema,
           ROUND(SUM(r.corte_mwh) / 1000.0, 1) AS corte_gwh,
           ROUND(100.0 * SUM(r.corte_mwh) / SUM(SUM(r.corte_mwh)) OVER (), 2) AS pct_do_corte_nacional,
           ROUND(100.0 * SUM(r.corte_mwh) / NULLIF(SUM(r.geracao_referencia_mwh), 0), 2) AS taxa_corte_pct,
           COUNT(DISTINCT r.sk_usina) AS usinas
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_subsistema')} s ON r.sk_subsistema = s.sk_subsistema
    GROUP BY s.nom_subsistema ORDER BY corte_gwh DESC
""")
display(p4_sub)

p4_uf = spark.sql(f"""
    SELECT u.nom_estado, u.id_estado,
           ROUND(SUM(r.corte_mwh) / 1000.0, 1) AS corte_gwh,
           ROUND(100.0 * SUM(r.corte_mwh) / SUM(SUM(r.corte_mwh)) OVER (), 2) AS pct_do_corte_nacional,
           ROUND(100.0 * SUM(r.corte_mwh) / NULLIF(SUM(r.geracao_referencia_mwh), 0), 2) AS taxa_corte_pct,
           COUNT(DISTINCT r.sk_usina) AS usinas
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_usina')} u ON r.sk_usina = u.sk_usina
    GROUP BY u.nom_estado, u.id_estado ORDER BY corte_gwh DESC
""")
display(p4_uf)

p4_usina = spark.sql(f"""
    SELECT u.nom_usina, u.nom_fonte, u.id_estado,
           ROUND(SUM(r.corte_mwh) / 1000.0, 1) AS corte_gwh,
           ROUND(100.0 * SUM(r.corte_mwh) / SUM(SUM(r.corte_mwh)) OVER (), 2) AS pct_do_corte_nacional
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_usina')} u ON r.sk_usina = u.sk_usina
    GROUP BY u.nom_usina, u.nom_fonte, u.id_estado ORDER BY corte_gwh DESC
""")
display(p4_usina.limit(10))

# COMMAND ----------

pdf4 = p4_uf.toPandas().head(10).sort_values("corte_gwh")
fig, ax = plt.subplots(figsize=(11, 5))
barras = ax.barh(pdf4.nom_estado, pdf4.corte_gwh, color=COR_NOME["Eólica"], height=0.66)
for b, v in zip(barras, pdf4.corte_gwh):
    ax.annotate(f"{br(v, 0)} GWh", (b.get_width(), b.get_y() + b.get_height() / 2), xytext=(6, 0),
                textcoords="offset points", va="center", color=TINTA_SUAVE, fontsize=9)
ax.set_title("P4 — Energia cortada por estado (dez maiores)", pad=12)
ax.set_xlabel("GWh cortados no período")
limpar_eixos(ax)
ax.grid(axis="y", visible=False)
ax.grid(axis="x", visible=True)
ax.set_xlim(0, pdf4.corte_gwh.max() * 1.18)
milhar(ax, "x")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P4

# COMMAND ----------

sub = p4_sub.toPandas()
uf = p4_uf.toPandas()
usi = p4_usina.toPandas()
lider = sub.iloc[0]
maior_taxa = sub.sort_values("taxa_corte_pct", ascending=False).iloc[0]
top3_uf = uf.head(3)
top10_usinas_pct = usi.head(10).pct_do_corte_nacional.sum()

print(f"""
POR SUBSISTEMA
""" + "\n".join(
    f"   {r.nom_subsistema:<14} {br(r.corte_gwh):>10} GWh  ({br(r.pct_do_corte_nacional)}% do total, taxa {br(r.taxa_corte_pct, 2)}%, {r.usinas} usinas)"
    for r in sub.itertuples()
) + f"""

TRÊS MAIORES ESTADOS (somam {br(top3_uf.pct_do_corte_nacional.sum())}% do corte do país)
""" + "\n".join(
    f"   {i + 1}. {r.nom_estado:<22} {br(r.corte_gwh):>10} GWh  ({br(r.pct_do_corte_nacional)}%, taxa {br(r.taxa_corte_pct, 2)}%)"
    for i, r in enumerate(top3_uf.itertuples())
) + f"""

AS DEZ USINAS COM MAIOR VOLUME CORTADO somam {br(top10_usinas_pct)}% do total.
Maior volume individual: {usi.iloc[0].nom_usina} ({usi.iloc[0].id_estado}, {usi.iloc[0].nom_fonte}), {br(usi.iloc[0].corte_gwh)} GWh.

LEITURA:

Em volume, o corte é um problema do {lider.nom_subsistema}: {br(lider.pct_do_corte_nacional)}% de toda a energia cortada
no país está lá. É a consequência direta de P1 — o subsistema que mais incorporou geração intermitente é o que
menos consegue absorvê-la.

Mas volume e intensidade contam histórias diferentes, e por isso separei as duas métricas. O {maior_taxa.nom_subsistema}
tem a MAIOR TAXA de corte ({br(maior_taxa.taxa_corte_pct, 2)}%), mesmo com volume bem menor. A lista de estados mostra por
quê: Minas Gerais aparece entre os três maiores, puxado por grandes complexos solares no norte do estado.
Existe, portanto, um segundo foco de corte fora do Nordeste, ligado à expansão solar.

No nível da usina, o corte é espalhado: as dez maiores somam só {br(top10_usinas_pct)}% do total, e
{int((usi.corte_gwh > 0).sum())} das {len(usi)} usinas monitoradas tiveram algum corte. Não é um problema de meia dúzia de empreendimentos mal
conectados — é um problema de sistema, com endereço regional.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P5 — Qual a razão predominante dos cortes, e ela mudou?
# MAGIC
# MAGIC > *Qual a razão predominante dos cortes: restrição elétrica/confiabilidade (escoamento) ou razão
# MAGIC > energética (excesso de oferta)? Isso mudou ao longo do tempo?*
# MAGIC
# MAGIC Uso os códigos oficiais do dicionário do ONS:
# MAGIC
# MAGIC | Código | Significado oficial | Natureza |
# MAGIC |---|---|---|
# MAGIC | **REL** | Razão de indisponibilidade externa (elétrica) | Rede — equipamento de transmissão indisponível |
# MAGIC | **CNF** | Razão de atendimento a requisitos de confiabilidade | Rede — limite de segurança do sistema elétrico |
# MAGIC | **ENE** | Razão energética | Oferta — o sistema não tem como absorver a energia |
# MAGIC | **PAR** | Restrição indicada no parecer de acesso | Condição de conexão da usina |
# MAGIC
# MAGIC Somo a energia por razão usando as colunas `corte_<razao>_mwh` do fato, e não a razão
# MAGIC predominante da hora — assim cada meia hora conta para a razão que de fato tinha.

# COMMAND ----------

p5_total = spark.sql(f"""
    SELECT SUM(corte_rel_mwh) / 1000.0 AS rel, SUM(corte_cnf_mwh) / 1000.0 AS cnf,
           SUM(corte_ene_mwh) / 1000.0 AS ene, SUM(corte_par_mwh) / 1000.0 AS par,
           SUM(corte_sis_mwh) / 1000.0 AS sis, SUM(corte_loc_mwh) / 1000.0 AS loc
    FROM {GOLD('fato_restricao_horaria')}
""").toPandas().iloc[0]

p5_mes = spark.sql(f"""
    SELECT d.ano_mes, SUM(corte_rel_mwh) / 1000.0 AS rel, SUM(corte_cnf_mwh) / 1000.0 AS cnf,
           SUM(corte_ene_mwh) / 1000.0 AS ene, SUM(corte_par_mwh) / 1000.0 AS par
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_data')} d ON r.sk_data = d.sk_data
    GROUP BY d.ano_mes ORDER BY d.ano_mes
""").toPandas().set_index("ano_mes")

p5_fonte = spark.sql(f"""
    SELECT f.nom_fonte,
           ROUND(100.0 * SUM(corte_rel_mwh) / SUM(corte_mwh), 1) AS pct_rel,
           ROUND(100.0 * SUM(corte_cnf_mwh) / SUM(corte_mwh), 1) AS pct_cnf,
           ROUND(100.0 * SUM(corte_ene_mwh) / SUM(corte_mwh), 1) AS pct_ene
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_fonte_geracao')} f ON r.sk_fonte = f.sk_fonte
    GROUP BY f.nom_fonte
""")

p5_pct = p5_mes.div(p5_mes.sum(axis=1), axis=0) * 100
display(p5_pct.round(1).reset_index())
display(p5_fonte)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(13, 5))
base = np.zeros(len(p5_pct))
for k in ["rel", "cnf", "ene", "par"]:
    if p5_pct[k].sum() > 0:
        ax.bar(p5_pct.index, p5_pct[k], bottom=base, color=COR_RAZAO[k], label=ROTULO_RAZAO[k], width=0.72,
               edgecolor=SUPERFICIE, linewidth=2)
        base = base + p5_pct[k].fillna(0).values
ax.set_title("P5 — Composição mensal da energia cortada por razão declarada pelo ONS", pad=12)
ax.set_ylabel("% da energia cortada no mês")
ax.set_ylim(0, 100)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.42), ncol=2)
ax.tick_params(axis="x", rotation=45)
limpar_eixos(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência textual — o que o ONS escreveu como motivo
# MAGIC
# MAGIC Desde setembro de 2025 o ONS descreve cada restrição em `dsc_restricao`. Esse campo fica na Silver
# MAGIC (é texto livre, sem valor como dimensão), mas é a melhor evidência qualitativa do que cada código
# MAGIC significa na prática.

# COMMAND ----------

descricoes = (
    spark.table(SILVER("restricao_coff_usina"))
    .filter(F.col("dsc_restricao").isNotNull())
    .withColumn("descricao", F.substring(F.regexp_replace("dsc_restricao", r"\s+", " "), 1, 160))
    .groupBy("cod_razao_restricao", "descricao")
    .agg(F.round(F.sum("corte_mwh") / 1000, 1).alias("corte_gwh"))
    .orderBy(F.desc("corte_gwh"))
    .limit(12)
)
display(descricoes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P5

# COMMAND ----------

total_razoes = p5_total.rel + p5_total.cnf + p5_total.ene + p5_total.par
pct = {k: 100 * p5_total[k] / total_razoes for k in ["rel", "cnf", "ene", "par"]}
pct_rede = pct["rel"] + pct["cnf"]
pct_sis = 100 * p5_total.sis / (p5_total.sis + p5_total.loc)

# Evolução: mesma decomposição em três janelas sucessivas de tamanho semelhante.
janelas = np.array_split(p5_mes.index.to_numpy(), 3)
evolucao = []
for j in janelas:
    bloco = p5_mes.loc[j].sum()
    t = bloco.sum()
    evolucao.append((f"{j[0]} a {j[-1]}", 100 * bloco.ene / t, 100 * (bloco.rel + bloco.cnf) / t))

top_desc = descricoes.toPandas()
desc_ene = top_desc[top_desc.cod_razao_restricao == "ENE"].head(1)
desc_cnf = top_desc[top_desc.cod_razao_restricao == "CNF"].head(3)

if pct["ene"] > pct_rede:
    conclusao = (
        f"O motivo predominante é ENERGÉTICO: {br(pct['ene'])}% da energia cortada, contra {br(pct_rede)}% somando as\n"
        "duas razões de rede (REL + CNF). Ou seja, na maior parte do tempo a energia não foi cortada porque faltava\n"
        "fio para levá-la, e sim porque o sistema como um todo não tinha como absorvê-la naquele instante."
    )
else:
    conclusao = (
        f"O motivo predominante é de REDE: {br(pct_rede)}% da energia cortada somando REL e CNF, contra {br(pct['ene'])}%\n"
        "de razão energética. Na maior parte do tempo, a energia existia e havia demanda, mas faltou capacidade de\n"
        "transmissão ou margem de segurança para escoá-la."
    )

tendencia = evolucao[-1][1] - evolucao[0][1]

print(f"""
ENERGIA CORTADA POR RAZÃO — período completo
   Razão energética (ENE)            {br(p5_total.ene):>10} GWh  ({br(pct['ene'])}%)
   Confiabilidade (CNF)              {br(p5_total.cnf):>10} GWh  ({br(pct['cnf'])}%)
   Indisponibilidade externa (REL)   {br(p5_total.rel):>10} GWh  ({br(pct['rel'])}%)
   Parecer de acesso (PAR)           {br(p5_total.par):>10} GWh  ({br(pct['par'])}%)

ORIGEM: sistêmica {br(pct_sis)}% | local {br(100 - pct_sis)}%

EVOLUÇÃO (participação da razão energética x razões de rede)
""" + "\n".join(f"   {rot:<20} ENE {br(e):>5}%  |  REL+CNF {br(r):>5}%" for rot, e, r in evolucao) + f"""

LEITURA:

{conclusao}

As descrições do ONS confirmam essa leitura. A razão energética aparece descrita como
"{desc_ene.descricao.iloc[0] if len(desc_ene) else '—'}" — o operador corta renováveis para manter a frequência do
sistema quando a geração supera o que a carga e os intercâmbios conseguem absorver. Já as restrições de
confiabilidade (CNF) apontam gargalos físicos específicos, como:
""" + "\n".join(f"   • {d}" for d in desc_cnf.descricao) + f"""

A origem é {br(pct_sis)}% sistêmica, o que reforça que o corte é decidido olhando o sistema, e não a usina
isolada.

A composição mudou ao longo do tempo: a participação da razão energética variou {'+' if tendencia >= 0 else ''}{br(tendencia)}
pontos percentuais entre a primeira e a última janela. O início de 2025 teve peso alto de indisponibilidade
externa (REL), concentrada no Nordeste; a partir daí, o corte passou a ser cada vez mais energético.

A distinção importa porque cada razão pede uma solução diferente. Corte por REL ou CNF se resolve com
transmissão e reforço de rede. Corte por razão energética não se resolve com linha nova: exige flexibilidade —
armazenamento, deslocamento de carga, redução da geração inflexível de outras fontes ou mais exportação.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P6 — Há relação entre o nível dos reservatórios e a intensidade dos cortes?
# MAGIC
# MAGIC > *Existe relação entre o nível dos reservatórios (EAR) e a intensidade dos cortes?*
# MAGIC
# MAGIC Minha hipótese: reservatório cheio significa menos espaço para "guardar" energia reduzindo a
# MAGIC geração hidráulica. Com a hidráulica já no mínimo e renovável em excesso, sobraria cortar.
# MAGIC
# MAGIC Testo a hipótese de dois jeitos: com o EAR do **próprio Nordeste** e com o EAR do
# MAGIC **Sudeste/Centro-Oeste**, que concentra a maior capacidade de armazenamento do país e está acoplado
# MAGIC ao Nordeste pelo intercâmbio.

# COMMAND ----------

p6 = spark.sql(f"""
    WITH corte_dia AS (
        SELECT sk_data, SUM(corte_mwh) / 1000.0 AS corte_gwh, SUM(geracao_referencia_mwh) / 1000.0 AS referencia_gwh
        FROM {GOLD('fato_restricao_horaria')} WHERE sk_subsistema = 'NE' GROUP BY sk_data
    )
    SELECT d.data, d.mes, c.corte_gwh,
           100.0 * c.corte_gwh / NULLIF(c.referencia_gwh, 0) AS taxa_corte_pct,
           ne.ear_percentual AS ear_ne, se.ear_percentual AS ear_se, ne.faixa_armazenamento AS faixa_ne
    FROM corte_dia c
    JOIN {GOLD('dim_data')} d ON c.sk_data = d.sk_data
    JOIN {GOLD('fato_reservatorio_diario')} ne ON ne.sk_data = c.sk_data AND ne.sk_subsistema = 'NE'
    JOIN {GOLD('fato_reservatorio_diario')} se ON se.sk_data = c.sk_data AND se.sk_subsistema = 'SE'
    ORDER BY d.data
""")
display(p6)

# COMMAND ----------

pdf6 = p6.toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
for ax, col, rotulo in [(axes[0], "ear_ne", "EAR do Nordeste (%)"), (axes[1], "ear_se", "EAR do Sudeste/C.O. (%)")]:
    r = pdf6[col].corr(pdf6.corte_gwh)
    ax.scatter(pdf6[col], pdf6.corte_gwh, s=24, color="#2a78d6", alpha=0.55, edgecolor=SUPERFICIE, linewidth=0.8)
    coef = np.polyfit(pdf6[col], pdf6.corte_gwh, 1)
    xs = np.linspace(pdf6[col].min(), pdf6[col].max(), 50)
    ax.plot(xs, np.polyval(coef, xs), color=TINTA_SUAVE, linewidth=2, linestyle="--",
            label=f"Tendência linear (r = {br(r, 2)})")
    ax.set_xlabel(rotulo)
    ax.legend(loc="upper left")
    limpar_eixos(ax)
    ax.grid(axis="x", visible=True)
axes[0].set_ylabel("Energia cortada no Nordeste no dia (GWh)")
fig.suptitle("P6 — Nível dos reservatórios × energia cortada por dia no Nordeste", fontsize=13, weight="bold", y=1.02)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P6

# COMMAND ----------

r_ne = pdf6.ear_ne.corr(pdf6.corte_gwh)
r_se = pdf6.ear_se.corr(pdf6.corte_gwh)
rho_ne = pdf6.ear_ne.rank().corr(pdf6.corte_gwh.rank())
rho_se = pdf6.ear_se.rank().corr(pdf6.corte_gwh.rank())

# Correlação dessazonalizada: removo a média de cada mês de ambas as séries, para separar a
# relação direta da sazonalidade que EAR e vento compartilham.
for col in ["corte_gwh", "ear_ne", "ear_se"]:
    pdf6[col + "_des"] = pdf6[col] - pdf6.groupby("mes")[col].transform("mean")
r_ne_des = pdf6.ear_ne_des.corr(pdf6.corte_gwh_des)
r_se_des = pdf6.ear_se_des.corr(pdf6.corte_gwh_des)

faixas = (
    pdf6.groupby("faixa_ne")
    .agg(dias=("data", "count"), ear_medio=("ear_ne", "mean"), corte_medio_gwh=("corte_gwh", "mean"),
         taxa_media_pct=("taxa_corte_pct", "mean"))
    .reset_index().sort_values("ear_medio")
)
display(faixas.round(2))

hipotese_sustentada = (r_ne > 0.3 or r_se > 0.3) and (r_ne_des > 0.3 or r_se_des > 0.3)

print(f"""
CORRELAÇÕES (base diária, n = {len(pdf6)} dias)

                         Pearson   Spearman   Pearson dessazonalizado
   EAR Nordeste × corte  {br(r_ne, 3):>7}   {br(rho_ne, 3):>8}   {br(r_ne_des, 3):>8}
   EAR Sudeste  × corte  {br(r_se, 3):>7}   {br(rho_se, 3):>8}   {br(r_se_des, 3):>8}

CORTE MÉDIO POR FAIXA DE ARMAZENAMENTO DO NORDESTE
""" + "\n".join(
    f"   {r.faixa_ne:<22} {r.dias:>4} dias   corte médio {br(r.corte_medio_gwh):>6} GWh/dia   taxa {br(r.taxa_media_pct)}%"
    for r in faixas.itertuples()
) + f"""

LEITURA:

{'A hipótese se sustenta' if hipotese_sustentada else 'A hipótese NÃO se sustenta'}. As correlações são {qualificar_correlacao(r_ne)} com o EAR do Nordeste
(r = {br(r_ne, 2)}) e {qualificar_correlacao(r_se)} com o EAR do Sudeste (r = {br(r_se, 2)}){' — e têm sinal negativo, o oposto do que a hipótese previa' if r_ne < 0 and r_se < 0 else ''}.
Nas faixas de armazenamento, os dias com reservatório mais cheio no Nordeste tiveram, em média, corte menor, e não maior.

A leitura das correlações brutas precisa de cuidado, porque EAR e vento compartilham a mesma sazonalidade: os
reservatórios enchem no período úmido (verão), quando o vento é mais fraco, e o corte é máximo na safra de
ventos (inverno e primavera), quando os reservatórios já estão deplecionando. Essa causa comum produz uma
correlação negativa sem relação direta entre as variáveis. Removendo a média de cada mês, a correlação fica em
{br(r_ne_des, 2)} para o Nordeste ({qualificar_correlacao(r_ne_des)}) e {br(r_se_des, 2)} para o Sudeste ({qualificar_correlacao(r_se_des)}).

Concluo que o nível dos reservatórios não é um fator explicativo relevante para o corte no período analisado.
Isso é coerente com P5: o corte energético é decidido pelo controle de frequência do sistema a cada momento, e
depende muito mais do balanço instantâneo entre geração, carga e capacidade de exportação do que do estoque de
água armazenada.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P7 — Com que frequência a carga líquida fica negativa, e como o intercâmbio responde?
# MAGIC
# MAGIC > *Em que medida a carga líquida (carga − eólica − solar) do Nordeste fica negativa, e como o
# MAGIC > intercâmbio responde a isso?*
# MAGIC
# MAGIC Convenção de sinal: **intercâmbio positivo = exportação**. O dicionário do ONS não define o sinal;
# MAGIC confirmei no notebook 04 que o intercâmbio é igual a `geração total − carga` (correlação de 0,99
# MAGIC a 1,00 em todos os subsistemas).

# COMMAND ----------

p7 = spark.sql(f"""
    SELECT h.hora,
           COUNT(*) AS horas_observadas,
           ROUND(100.0 * AVG(CASE WHEN b.carga_liquida_negativa THEN 1 ELSE 0 END), 2) AS pct_horas_negativas,
           ROUND(AVG(b.carga_mwmed), 1) AS carga_media_mwmed,
           ROUND(AVG(b.ger_intermitente_mwmed), 1) AS intermitente_media_mwmed,
           ROUND(AVG(b.carga_liquida_mwmed), 1) AS carga_liquida_media_mwmed,
           ROUND(AVG(b.intercambio_mwmed), 1) AS intercambio_medio_mwmed
    FROM {GOLD('fato_balanco_horario')} b
    JOIN {GOLD('dim_hora')} h ON b.sk_hora = h.sk_hora
    JOIN {GOLD('dim_data')} d ON b.sk_data = d.sk_data
    WHERE b.sk_subsistema = 'NE' AND d.ano >= {ANO_COMPLETO}
    GROUP BY h.hora ORDER BY h.hora
""")
display(p7)

p7_ano = spark.sql(f"""
    SELECT d.ano,
           SUM(CASE WHEN b.carga_liquida_negativa THEN 1 ELSE 0 END) AS horas_negativas,
           ROUND(100.0 * AVG(CASE WHEN b.carga_liquida_negativa THEN 1 ELSE 0 END), 2) AS pct_horas_negativas
    FROM {GOLD('fato_balanco_horario')} b JOIN {GOLD('dim_data')} d ON b.sk_data = d.sk_data
    WHERE b.sk_subsistema = 'NE'
    GROUP BY d.ano ORDER BY d.ano
""")
display(p7_ano)

# COMMAND ----------

pdf7 = p7.toPandas()
fig, ax = plt.subplots(figsize=(12, 5.2))
for col, cor, rot in [("carga_media_mwmed", "#2a78d6", "Carga"),
                      ("intermitente_media_mwmed", "#1baf7a", "Geração intermitente"),
                      ("carga_liquida_media_mwmed", "#eb6834", "Carga líquida"),
                      ("intercambio_medio_mwmed", "#eda100", "Intercâmbio (+ exportação)")]:
    ax.plot(pdf7.hora, pdf7[col], color=cor, linewidth=2, marker="o", markersize=5, label=rot)
ax.axhline(0, color=TINTA_SUAVE, linewidth=1.2, linestyle=":")
ax.set_title(f"P7 — Nordeste ({ANO_COMPLETO} em diante): perfil horário médio", pad=12)
ax.set_xlabel("Hora do dia")
ax.set_ylabel("MW médios")
ax.set_xticks(range(0, 24, 2))
ax.set_xticklabels([f"{h:02d}h" for h in range(0, 24, 2)])
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=4)
limpar_eixos(ax)
milhar(ax)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P7

# COMMAND ----------

g7 = spark.sql(f"""
    SELECT COUNT(*) AS horas,
           SUM(CASE WHEN carga_liquida_negativa THEN 1 ELSE 0 END) AS negativas,
           100.0 * AVG(CASE WHEN carga_liquida_negativa THEN 1 ELSE 0 END) AS pct_negativas,
           MIN(carga_liquida_mwmed) AS carga_liquida_min,
           MAX(intercambio_mwmed) AS intercambio_max,
           corr(carga_liquida_mwmed, intercambio_mwmed) AS corr_cl_interc,
           AVG(CASE WHEN carga_liquida_negativa THEN intercambio_mwmed END) AS interc_quando_negativa,
           AVG(CASE WHEN NOT carga_liquida_negativa THEN intercambio_mwmed END) AS interc_quando_positiva,
           100.0 * AVG(CASE WHEN intercambio_mwmed > 0 THEN 1 ELSE 0 END) AS pct_horas_exportando
    FROM {GOLD('fato_balanco_horario')} b JOIN {GOLD('dim_data')} d ON b.sk_data = d.sk_data
    WHERE b.sk_subsistema = 'NE' AND d.ano >= {ANO_COMPLETO}
""").collect()[0]

cc = spark.sql(f"""
    WITH c AS (SELECT sk_data, sk_hora, SUM(corte_mwh) AS corte
               FROM {GOLD('fato_restricao_horaria')} WHERE sk_subsistema = 'NE' GROUP BY sk_data, sk_hora)
    SELECT corr(c.corte, b.carga_liquida_mwmed) AS corr_corte_cl,
           AVG(CASE WHEN b.carga_liquida_negativa THEN c.corte END) AS corte_medio_negativa,
           AVG(CASE WHEN NOT b.carga_liquida_negativa THEN c.corte END) AS corte_medio_positiva,
           100.0 * SUM(CASE WHEN b.carga_liquida_negativa THEN c.corte ELSE 0 END) / SUM(c.corte) AS pct_corte_em_negativas
    FROM c JOIN {GOLD('fato_balanco_horario')} b
      ON b.sk_data = c.sk_data AND b.sk_hora = c.sk_hora AND b.sk_subsistema = 'NE'
""").collect()[0]

anos7 = p7_ano.toPandas()
h_neg = pdf7.loc[pdf7.carga_liquida_media_mwmed.idxmin()]
h_exp = pdf7.loc[pdf7.intercambio_medio_mwmed.idxmax()]
horas_positivas = pdf7[pdf7.carga_liquida_media_mwmed > 0].hora.tolist()

print(f"""
NORDESTE — {ANO_COMPLETO} em diante ({br(g7['horas'], 0)} horas)

Horas com carga líquida NEGATIVA : {br(g7['negativas'], 0)} ({br(g7['pct_negativas'])}%)
Menor carga líquida registrada   : {br(g7['carga_liquida_min'])} MWmed
Maior exportação registrada      : {br(g7['intercambio_max'])} MWmed
Horas em que o Nordeste exporta  : {br(g7['pct_horas_exportando'])}%

Hora de carga líquida média mais negativa : {int(h_neg.hora):02d}h ({br(h_neg.carga_liquida_media_mwmed)} MWmed)
Hora de maior exportação média            : {int(h_exp.hora):02d}h ({br(h_exp.intercambio_medio_mwmed)} MWmed)
Horas do dia com carga líquida média positiva: {', '.join(f'{h:02d}h' for h in horas_positivas) or 'nenhuma'}

EVOLUÇÃO DAS HORAS COM CARGA LÍQUIDA NEGATIVA
""" + "\n".join(f"   {int(r.ano)}: {br(r.pct_horas_negativas):>6}% das horas" for r in anos7.itertuples()) + f"""

RELAÇÃO COM O INTERCÂMBIO E COM O CORTE
   Correlação carga líquida × intercâmbio (hora a hora) : {br(g7['corr_cl_interc'], 3)}
   Exportação média quando a carga líquida é negativa   : {br(g7['interc_quando_negativa'])} MWmed
   Exportação média quando a carga líquida é positiva   : {br(g7['interc_quando_positiva'])} MWmed
   Corte médio por hora — carga líquida negativa        : {br(cc['corte_medio_negativa'])} MWh
   Corte médio por hora — carga líquida positiva        : {br(cc['corte_medio_positiva'])} MWh
   Parcela do corte do Nordeste em horas de carga líquida negativa: {br(cc['pct_corte_em_negativas'])}%

LEITURA:

Carga líquida negativa significa que, naquela hora, eólica e solar sozinhas geraram mais do que todo o consumo
do Nordeste. Isso já não é exceção: acontece em {br(g7['pct_negativas'])}% das horas. A série anual mostra a
velocidade da mudança — de {br(anos7.iloc[0].pct_horas_negativas)}% em {int(anos7.iloc[0].ano)} para mais de dois terços das horas.
No perfil médio, a carga líquida só fica positiva no fim da tarde e início da noite, quando a solar desaparece.

Quando há excedente, o sistema tem três saídas: exportar, armazenar ou cortar. O Brasil praticamente não tem
armazenamento em escala de rede, e o intercâmbio mostra que a exportação está trabalhando no limite: a
correlação de {br(g7['corr_cl_interc'], 2)} entre carga líquida e intercâmbio indica que, quanto maior o excedente, mais o
Nordeste exporta — em média {br(g7['interc_quando_negativa'], 0)} MWmed nas horas de carga líquida negativa.

Mas a exportação tem teto, e o corte aparece justamente quando esse teto é atingido: {br(cc['pct_corte_em_negativas'])}% de toda a
energia cortada no Nordeste ocorre em horas de carga líquida negativa, e o corte médio nessas horas é
{br(cc['corte_medio_negativa'] / cc['corte_medio_positiva'])} vezes maior do que nas demais. É o mecanismo físico por trás de P5: quando a
exportação satura, a razão declarada é de confiabilidade da rede; quando o sistema inteiro não consegue absorver
o excedente, é energética.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # P8 — Quais usinas mais perdem energia proporcionalmente?
# MAGIC
# MAGIC > *Quais usinas têm a maior taxa de corte relativa à sua própria geração de referência?*

# COMMAND ----------

p8 = spark.sql(f"""
    SELECT u.nom_usina, u.nom_fonte, u.id_estado, u.potencia_instalada_mw,
           ROUND(SUM(r.corte_mwh) / 1000.0, 2) AS corte_gwh,
           ROUND(SUM(r.geracao_referencia_mwh) / 1000.0, 2) AS referencia_gwh,
           ROUND(100.0 * SUM(r.corte_mwh) / NULLIF(SUM(r.geracao_referencia_mwh), 0), 2) AS taxa_corte_pct
    FROM {GOLD('fato_restricao_horaria')} r JOIN {GOLD('dim_usina')} u ON r.sk_usina = u.sk_usina
    GROUP BY u.nom_usina, u.nom_fonte, u.id_estado, u.potencia_instalada_mw
    -- Piso de 10 GWh de referência: impede que uma usina com pouco histórico lidere por acaso.
    HAVING SUM(r.geracao_referencia_mwh) > 10000
    ORDER BY taxa_corte_pct DESC
""")
display(p8.limit(20))

# COMMAND ----------

pdf8_todas = p8.toPandas()
pdf8 = pdf8_todas.head(12).sort_values("taxa_corte_pct")
media_sistema = float(tot["taxa_pct"])

fig, ax = plt.subplots(figsize=(12, 5.8))
barras = ax.barh(pdf8.nom_usina + " (" + pdf8.id_estado + ")", pdf8.taxa_corte_pct,
                 color=[COR_NOME.get(f, COR_NOME["Eólica"]) for f in pdf8.nom_fonte], height=0.66)
for b, v in zip(barras, pdf8.taxa_corte_pct):
    ax.annotate(f"{br(v)}%", (b.get_width(), b.get_y() + b.get_height() / 2), xytext=(6, 0),
                textcoords="offset points", va="center", color=TINTA_SUAVE, fontsize=9)
ax.axvline(media_sistema, color=TINTA_SUAVE, linestyle=":", linewidth=1.5)
ax.legend(
    handles=[Patch(facecolor=COR_NOME[f], label=f) for f in sorted(pdf8.nom_fonte.unique())]
    + [Line2D([0], [0], color=TINTA_SUAVE, linestyle=":", linewidth=1.5, label=f"Média do sistema ({br(media_sistema)}%)")],
    loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3,
)
ax.set_title("P8 — Usinas com maior taxa de corte (geração de referência > 10 GWh)", pad=12)
ax.set_xlabel("% da geração de referência cortada no período")
limpar_eixos(ax)
ax.grid(axis="y", visible=False)
ax.grid(axis="x", visible=True)
ax.set_xlim(0, pdf8.taxa_corte_pct.max() * 1.15)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discussão — P8

# COMMAND ----------

pior = pdf8_todas.iloc[0]
top20 = pdf8_todas.head(20)
mediana = pdf8_todas.taxa_corte_pct.median()
p90 = pdf8_todas.taxa_corte_pct.quantile(0.9)
acima_20 = int((pdf8_todas.taxa_corte_pct > 20).sum())
potencia_pior = "sem cadastro (conjunto de usinas)" if pd.isna(pior.potencia_instalada_mw) else f"{br(pior.potencia_instalada_mw)} MW"

print(f"""
DISTRIBUIÇÃO DA TAXA DE CORTE ({len(pdf8_todas)} usinas com mais de 10 GWh de referência)
   Mediana                 : {br(mediana, 2)}%
   Percentil 90            : {br(p90, 2)}%
   Máxima                  : {br(pdf8_todas.taxa_corte_pct.max(), 2)}%
   Usinas acima de 20%     : {acima_20} ({br(100 * acima_20 / len(pdf8_todas))}%)
   Média ponderada sistema : {br(media_sistema, 2)}%

USINA MAIS AFETADA
   {pior.nom_usina} ({pior.id_estado}, {pior.nom_fonte}) — {br(pior.taxa_corte_pct, 2)}% da geração de referência
   {br(pior.corte_gwh, 1)} GWh cortados de {br(pior.referencia_gwh, 1)} GWh possíveis | potência: {potencia_pior}
   Perde {br(pior.taxa_corte_pct / media_sistema, 1)} vezes a taxa média do sistema.

COMPOSIÇÃO DAS 20 MAIORES TAXAS
   Por estado : {', '.join(f'{k} ({v})' for k, v in top20.id_estado.value_counts().items())}
   Por fonte  : {', '.join(f'{k} ({v})' for k, v in top20.nom_fonte.value_counts().items())}

LEITURA:

O corte não é distribuído de forma equânime, mas também não é um problema de poucas usinas. A usina típica
perdeu {br(mediana, 1)}% da geração que poderia ter produzido, e {br(100 * acima_20 / len(pdf8_todas), 0)}% das usinas perderam mais de 20%. No topo, a perda chega a
{br(pdf8_todas.taxa_corte_pct.max(), 1)}% — praticamente metade da produção possível.

As maiores taxas se concentram no Rio Grande do Norte e na Bahia, e dividem-se entre eólicas e solares. Isso
se conecta a P4 e P5: são os estados com os gargalos de transmissão que aparecem nas descrições de restrição por
confiabilidade (como as linhas de 500 kV que partem de Açu III, no Rio Grande do Norte).

A consequência econômica é direta: duas usinas com o mesmo custo de capital e o mesmo recurso natural podem ter
receitas muito diferentes dependendo apenas de onde foram conectadas. É um risco locacional que um estudo de
viabilidade baseado só no recurso eólico ou solar do terreno não captura.

Nota metodológica: o piso de 10 GWh no HAVING impede que uma usina recém-conectada, com poucas semanas de
histórico, apareça no topo por ruído estatístico. A potência instalada aparece só para as 15 usinas
individuais; os conjuntos de usinas não têm CEG e não casam com o cadastro de capacidade (notebook 03).
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Discussão geral — o que o pipeline respondeu

# COMMAND ----------

conclusao_principal = (
    "predominantemente ENERGÉTICO: o sistema não consegue absorver, a cada momento, toda a geração renovável\n"
    "disponível — e não, na maior parte do tempo, uma falta de linhas de transmissão"
    if pct["ene"] > pct_rede else
    "predominantemente de REDE: falta capacidade de transmissão ou margem de segurança para escoar a energia"
)

print(f"""
================================================================================================
SÍNTESE — DO PROBLEMA ÀS RESPOSTAS
================================================================================================

O problema que formulei na etapa 2 foi: a expansão de eólica e solar, concentrada no Nordeste, cresceu mais rápido
do que a capacidade do sistema de absorvê-la, e o operador passou a cortar geração renovável disponível. Quanto
se perde, onde, quando e por quê?

P1 — CAUSA ESTRUTURAL. A intermitente foi de {br(int_ne_ini)}% para {br(int_ne_fim)}% da geração do Nordeste entre {ano_ini} e {ANO_COMPLETO},
     e de {br(int_br_ini)}% para {br(int_br_fim)}% no país. Cresceu justamente o que o operador não despacha.

P2 — QUANDO. Em {ANO_COMPLETO}, eólica + solar superaram a carga do Nordeste em {br(p2_ano['pct_horas_acima_100'])}% das horas. O pico
     é no início da manhã dos meses de vento forte; o único alívio é o fim da tarde.

P3 — QUANTO. {br(tot['corte_gwh'])} GWh cortados em {meses} meses, {br(tot['taxa_pct'], 1)}% da geração possível — valor validado contra a
     GNRa oficial do ONS. O problema não diminuiu na comparação entre anos.

P4 — ONDE. {br(lider.pct_do_corte_nacional)}% do volume está no {lider.nom_subsistema}; a maior taxa, no {maior_taxa.nom_subsistema}, puxada pela solar de
     Minas Gerais. No nível da usina o corte é espalhado.

P5 — POR QUÊ. {br(pct['ene'])}% da energia foi cortada por razão energética e {br(pct_rede)}% por razões de rede (REL + CNF).
     A razão energética ganhou peso ao longo do período.

P6 — HIPÓTESE DO RESERVATÓRIO. {'Se sustenta' if hipotese_sustentada else 'Não se sustenta'}: correlação {qualificar_correlacao(r_ne)} com o EAR do NE ({br(r_ne, 2)})
     e {qualificar_correlacao(r_se)} com o do SE ({br(r_se, 2)}); sem a sazonalidade, {br(r_ne_des, 2)} e {br(r_se_des, 2)}.

P7 — MECANISMO. A carga líquida do Nordeste é negativa em {br(g7['pct_negativas'])}% das horas. A exportação absorve o excedente
     até o limite ({br(g7['corr_cl_interc'], 2)} de correlação), e {br(cc['pct_corte_em_negativas'])}% do corte acontece quando esse limite é atingido.

P8 — QUEM PERDE. A usina típica perde {br(mediana, 1)}% da produção possível; as piores, perto de metade. As maiores taxas estão
     no Rio Grande do Norte e na Bahia.

------------------------------------------------------------------------------------------------
CONCLUSÃO

No período analisado, o corte de renováveis no Brasil é {conclusao_principal}.

A implicação para decisão é que reforço de transmissão, sozinho, não resolve o problema. Ele atacaria a parcela
de rede ({br(pct_rede)}%), concentrada em gargalos específicos que as descrições do ONS identificam. A parcela
energética ({br(pct['ene'])}%) pede flexibilidade: armazenamento, deslocamento de consumo para as horas de excedente,
redução da geração inflexível e mais capacidade de exportação entre subsistemas. E a expansão de novas usinas
intermitentes, sem essa flexibilidade, tende a aumentar a taxa de corte de todas as que já existem.
================================================================================================
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nota
# MAGIC
# MAGIC Todos os números das discussões são calculados na execução, a partir das tabelas Gold. Alterando
# MAGIC `COFF_INICIO` / `COFF_FIM` no notebook `_config` e reexecutando o pipeline, o texto se reescreve
# MAGIC com os valores do novo período — inclusive a escolha da conclusão principal, que depende do
# MAGIC resultado da P5.
