# Databricks notebook source
# MAGIC %md
# MAGIC # `_config` — Configuração compartilhada do pipeline
# MAGIC
# MAGIC Notebook de apoio, **não deve ser executado sozinho**. Os demais notebooks o carregam com
# MAGIC `%run ./_config`, o que injeta nesta sessão as constantes de nomeação (catálogo, schemas,
# MAGIC volume), o período de análise e as funções auxiliares de acesso à API do ONS.
# MAGIC
# MAGIC Centralizei isso aqui para evitar o erro clássico de pipeline acadêmico: o mesmo nome de tabela
# MAGIC escrito de três jeitos diferentes em três notebooks.

# COMMAND ----------

from datetime import datetime, date
import json
import urllib.request

# ---------------------------------------------------------------------------
# Semântica de conversão de tipos
# ---------------------------------------------------------------------------
# O Databricks liga o modo ANSI por padrão: um CAST inválido levanta exceção em
# vez de devolver NULL. Este pipeline assume a semântica clássica — converter o
# que der e deixar NULL no resto, para que a perda seja MEDIDA na etapa de
# qualidade (notebook 04) em vez de abortar a carga. Os registros com quebra de
# linha dentro de campo texto, que antes chegavam partidos, passaram a ser lidos
# inteiros com multiLine no notebook 01; o modo ANSI desligado permanece como
# proteção para qualquer valor malformado que a fonte venha a publicar.
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    print("Modo ANSI desligado — CAST inválido resulta em NULL (perda medida no notebook 04).")
except Exception as _e:
    print(f"[ATENÇÃO] Não foi possível desligar o modo ANSI: {_e}")

# ---------------------------------------------------------------------------
# Nomeação das camadas (Arquitetura Medalhão)
# ---------------------------------------------------------------------------
# Um catálogo do Unity Catalog para o projeto, um schema por camada.
# O notebook 00 faz fallback para o catálogo `workspace` caso a Free Edition
# não permita a criação de catálogos na conta em uso.
CATALOG = "mvp_ons"
SCH_BRONZE = "bronze"
SCH_SILVER = "silver"
SCH_GOLD = "gold"
VOLUME = "raw_ons"  # Volume gerenciado onde os arquivos crus são aterrissados

def esc_sql(texto: str) -> str:
    """Escapa aspas simples para interpolar texto com seguranca em literais SQL.

    Sem isso, uma descricao que contenha aspas simples (ex.: "sentinelas ('-', '')")
    quebra o COMMENT ON ... IS '...' com PARSE_SYNTAX_ERROR.
    """
    return texto.replace("'", "''")


def _fq(schema: str, obj: str = "") -> str:
    """Monta o nome totalmente qualificado `catalogo.schema.objeto`."""
    return f"{CATALOG}.{schema}" + (f".{obj}" if obj else "")

BRONZE = lambda t="": _fq(SCH_BRONZE, t)
SILVER = lambda t="": _fq(SCH_SILVER, t)
GOLD = lambda t="": _fq(SCH_GOLD, t)

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCH_BRONZE}/{VOLUME}"

# ---------------------------------------------------------------------------
# Janela temporal do MVP
# ---------------------------------------------------------------------------
# Balanço e reservatórios são arquivos anuais e leves (~4 MB/ano): janela larga.
ANO_INICIO = 2021
ANO_FIM = 2026

# Restrição (constrained-off) são arquivos MENSAIS e pesados (~23 MB/mês eólica,
# ~9 MB/mês solar). A janela é menor de propósito para o pipeline rodar em tempo
# razoável na Free Edition. Ampliar aqui é seguro — o pipeline é idempotente.
COFF_INICIO = "2025-01"
COFF_FIM = "2026-08"

# ---------------------------------------------------------------------------
# Fontes de dados — Portal de Dados Abertos do ONS (CKAN)
# ---------------------------------------------------------------------------
CKAN_API = "https://dados.ons.org.br/api/3/action/package_show?id={}"

DATASETS = {
    "balanco_energia_subsistema": {
        "package": "balanco-energia-subsistema",
        "periodicidade": "anual",
        "descricao": "Balanço de energia por subsistema em base horária",
    },
    "ear_diario_subsistema": {
        "package": "ear-diario-por-subsistema",
        "periodicidade": "anual",
        "descricao": "Energia Armazenada Verificada (EAR) diária por subsistema",
    },
    "restricao_coff_eolica": {
        "package": "restricao_coff_eolica_usi",
        "periodicidade": "mensal",
        "descricao": "Restrição de operação por constrained-off de usinas eólicas",
    },
    "restricao_coff_fotovoltaica": {
        "package": "restricao_coff_fotovoltaica",
        "periodicidade": "mensal",
        "descricao": "Restrição de operação por constrained-off de usinas fotovoltaicas",
    },
    "capacidade_geracao": {
        "package": "capacidade-geracao",
        "periodicidade": "unico",
        "descricao": "Capacidade instalada de geração por unidade geradora",
    },
}

# COMMAND ----------

def listar_recursos_csv(package_id: str) -> list:
    """Consulta a API CKAN do ONS e devolve os recursos CSV do dataset.

    Retorna uma lista de dicts com `name` e `url`. A URL do arquivo não é
    construída à mão de propósito: o ONS altera o caminho no S3 entre datasets
    (ex.: o pacote `restricao_coff_eolica_usi` publica em `restricao_coff_eolica_tm/`),
    então o único caminho confiável é o que a própria API devolve.
    """
    req = urllib.request.Request(
        CKAN_API.format(package_id),
        headers={"User-Agent": "MVP-EngDados-PUCRJ/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    if not payload.get("success"):
        raise RuntimeError(f"API CKAN não retornou sucesso para '{package_id}'")

    return [
        {"name": r.get("name"), "url": r.get("url")}
        for r in payload["result"]["resources"]
        if r.get("format") == "CSV"
    ]


def licenca_do_dataset(package_id: str) -> dict:
    """Devolve os metadados de licença do dataset — usado para evidenciar a
    conformidade de uso exigida na etapa 4.1."""
    req = urllib.request.Request(
        CKAN_API.format(package_id),
        headers={"User-Agent": "MVP-EngDados-PUCRJ/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        r = json.loads(resp.read().decode("utf-8"))["result"]
    return {
        "titulo": r.get("title"),
        "licenca": r.get("license_title"),
        "license_id": r.get("license_id"),
        "license_url": r.get("license_url"),
    }


def filtrar_por_periodo(recursos: list, periodicidade: str) -> list:
    """Filtra a lista de recursos para a janela temporal configurada.

    Os nomes dos arquivos do ONS carregam o período no final:
      anual  -> `..._2025.csv`
      mensal -> `..._2025_06.csv`
    """
    if periodicidade == "unico":
        return recursos

    selecionados = []
    for r in recursos:
        arquivo = r["url"].rsplit("/", 1)[-1].replace(".csv", "")

        if periodicidade == "anual":
            sufixo = arquivo.rsplit("_", 1)[-1]
            if sufixo.isdigit() and ANO_INICIO <= int(sufixo) <= ANO_FIM:
                selecionados.append(r)

        elif periodicidade == "mensal":
            partes = arquivo.rsplit("_", 2)
            if len(partes) >= 3 and partes[-2].isdigit() and partes[-1].isdigit():
                competencia = f"{partes[-2]}-{partes[-1]}"
                if COFF_INICIO <= competencia <= COFF_FIM:
                    selecionados.append(r)

    return sorted(selecionados, key=lambda x: x["url"])

# COMMAND ----------

# Metadados do subsistema, usados para construir a dimensão na camada Gold.
# Fonte: definição operativa do SIN (ONS). `SIN` é a linha agregada presente no
# arquivo de balanço e NÃO é um subsistema real — daí a flag `eh_agregado`.
SUBSISTEMAS = [
    ("N",   "NORTE",                 "Norte",         "AC, AM, AP, PA, RO, RR, TO",             False),
    ("NE",  "NORDESTE",              "Nordeste",      "AL, BA, CE, MA, PB, PE, PI, RN, SE",     False),
    ("SE",  "SUDESTE/CENTRO-OESTE",  "Sudeste/C.O.",  "DF, ES, GO, MG, MS, MT, RJ, SP",         False),
    ("S",   "SUL",                   "Sul",           "PR, RS, SC",                             False),
    ("SIN", "SISTEMA INTERLIGADO NACIONAL", "Brasil", "Agregado nacional",                      True),
]

# Metadados das fontes de geração, usados para construir a dimensão na camada Gold.
# `intermitente` marca as fontes cuja disponibilidade depende de recurso natural
# não controlável — é a chave das perguntas P2, P3 e P7.
FONTES_GERACAO = [
    # (codigo, nome, coluna_origem_no_balanco, renovavel, despachavel, intermitente)
    ("HID", "Hidráulica", "val_gerhidraulica", True,  True,  False),
    ("TER", "Térmica",    "val_gertermica",    False, True,  False),
    ("EOL", "Eólica",     "val_gereolica",     True,  False, True),
    ("SOL", "Solar",      "val_gersolar",      True,  False, True),
]

print(f"Configuração carregada — catálogo alvo: {CATALOG}")
print(f"  Camadas : {SCH_BRONZE} / {SCH_SILVER} / {SCH_GOLD}")
print(f"  Volume  : {VOLUME_PATH}")
print(f"  Período : balanço/EAR {ANO_INICIO}-{ANO_FIM} | constrained-off {COFF_INICIO} a {COFF_FIM}")
