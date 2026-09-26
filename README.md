# MVP — Engenharia de Dados

## Quanta energia renovável o Brasil está jogando fora, e por quê?

Construí um pipeline de dados de ponta a ponta na Databricks, sobre os dados abertos do
**ONS — Operador Nacional do Sistema Elétrico**, para dimensionar e explicar o corte
(*constrained-off*) de geração eólica e solar no Sistema Interligado Nacional (SIN).

| | |
|---|---|
| **Plataforma** | Databricks Free Edition (Unity Catalog + Delta Lake + Apache Spark) |
| **Arquitetura** | Medalhão — Bronze / Silver / Gold |
| **Modelagem** | Constelação de fatos (esquema estrela com dimensões conformes) |
| **Fonte** | Portal de Dados Abertos do ONS — 5 datasets, licença CC-BY |
| **Coleta** | Automatizada via API CKAN do ONS |
| **Volume** | 53 arquivos CSV, 1,15 GB, 6,8 milhões de registros na camada Bronze |
| **Janela** | Balanço e reservatórios: jan/2021 a set/2026 · Constrained-off: jan/2025 a ago/2026 |

### Resumo das respostas

- Em 20 meses, **61.438,6 GWh** de energia eólica e solar foram cortados — **21,0%** de tudo o que essas
  usinas poderiam ter gerado. Meu cálculo reproduz a apuração oficial do ONS (GNRa) com diferença de 0,0016%.
- **83,5%** do volume cortado está no Nordeste. A maior *taxa* de corte, porém, está no Sudeste/Centro-Oeste
  (25,5%), puxada pelos complexos solares de Minas Gerais.
- **59,6%** da energia foi cortada por **razão energética** (o sistema não conseguia absorvê-la), e 40,4% por
  razões de rede (confiabilidade e indisponibilidade externa). A razão energética ganhou peso ao longo do período.
- O nível dos reservatórios **não** explica o corte: correlações fracas e sem sustentação depois de remover
  a sazonalidade.
- A carga líquida do Nordeste foi **negativa em 69,3% das horas** desde 2025, e 84,9% do corte do Nordeste
  acontece justamente nessas horas.
- A usina típica perdeu **23,9%** da produção possível; as mais afetadas, perto de metade.

---

## Índice

1. [Contexto de Negócios e Perguntas (Etapa 2. e 4.1)](#1-contexto-de-negócios-e-perguntas-etapa-2-e-41)
2. [Carga dos Dados (Etapa 4.2)](#2-carga-dos-dados-etapa-42)
3. [Modelagem e Catálogo de Dados (Etapa 4.3)](#3-modelagem-e-catálogo-de-dados-etapa-43)
4. [Pipeline de Dados (Etapa 4.4)](#4-pipeline-de-dados-etapa-44)
5. [Qualidade de Dados (Etapa 4.5)](#5-qualidade-de-dados-etapa-45)
6. [Análise de Dados (Etapa 4.5)](#6-análise-de-dados-etapa-45)
7. [Autoavaliação](#7-autoavaliação)
8. [Como reproduzir](#8-como-reproduzir)

---

## 1. Contexto de Negócios e Perguntas (Etapa 2. e 4.1)

### 1.1 O problema

O Brasil construiu, em menos de uma década, um parque eólico e solar de escala mundial — e o concentrou
geograficamente. O Nordeste responde hoje pela maior parte da geração eólica do país, enquanto o consumo
permanece concentrado no Sudeste.

A rede de transmissão e a flexibilidade do sistema não acompanharam esse ritmo. O resultado é um fenômeno que
passou a aparecer com frequência crescente nos boletins de operação: o **constrained-off**, ou corte de geração.
O ONS determina que uma usina eólica ou solar gere abaixo do que poderia — não por falta de vento ou de sol, mas
porque a rede não consegue escoar aquela energia ou porque o sistema não tem como absorvê-la naquele instante.

É energia renovável, de usina já construída e contratada, que simplesmente deixa de existir. Para quem investiu,
é receita que não se realiza. Para o sistema, é um sintoma de desequilíbrio entre a expansão da geração e a
capacidade de absorvê-la.

**O problema que me propus a resolver:** dimensionar essa perda e explicá-la, com dados de operação verificados,
de modo a distinguir se o gargalo é de *transmissão* (falta de rede) ou de *flexibilidade* (falta de demanda,
armazenamento ou exportação) — porque cada diagnóstico aponta para um investimento diferente.

### 1.2 Por que escolhi este problema, e por que ele é de Engenharia de Dados

Escolhi o setor elétrico porque é um setor que conheço profissionalmente e porque o problema do corte é atual e
tem consequência econômica direta. Mas a pergunta não se responde olhando uma tabela. Para respondê-la, preciso:

- **Cruzar fontes de granularidades incompatíveis** — geração horária por subsistema, restrição semi-horária por
  usina e reservatório diário por subsistema;
- **Lidar com um volume que não cabe confortavelmente em uma planilha** — 6,8 milhões de registros e 1,15 GB;
- **Tratar problemas de qualidade não triviais** — incluindo uma linha agregada que dobra os totais e ausências
  de dados que têm significado (ver seção 5);
- **Construir um modelo que suporte perguntas ainda não formuladas**, e não apenas as oito abaixo.

É exatamente o tipo de problema em que a resposta analítica depende de o pipeline estar certo.

### 1.3 As perguntas

| # | Pergunta | Respondida em |
|---|---|---|
| **P1** | Como evoluiu a participação de cada fonte (hidráulica, térmica, eólica e solar) na geração, por subsistema, ao longo do período? | [Seção 6 — P1](#p1--participação-das-fontes-na-geração) |
| **P2** | Em quais horas do dia e meses do ano a geração intermitente (eólica + solar) atinge maior participação sobre a carga do subsistema? | [Seção 6 — P2](#p2--horas-e-meses-de-maior-participação-intermitente) |
| **P3** | Qual o volume total de energia eólica e solar cortada por *constrained-off*, e como evoluiu ao longo do tempo? | [Seção 6 — P3](#p3--volume-de-energia-cortada) |
| **P4** | Onde os cortes se concentram — quais subsistemas, estados e usinas? | [Seção 6 — P4](#p4--onde-os-cortes-se-concentram) |
| **P5** | Qual a razão predominante dos cortes: restrição elétrica/confiabilidade (escoamento) ou razão energética (excesso de oferta)? Isso mudou ao longo do tempo? | [Seção 6 — P5](#p5--razão-predominante-dos-cortes) |
| **P6** | Existe relação entre o nível dos reservatórios (EAR) e a intensidade dos cortes? | [Seção 6 — P6](#p6--reservatórios--cortes) |
| **P7** | Em que medida a carga líquida (carga − eólica − solar) do Nordeste fica negativa, e como o intercâmbio responde a isso? | [Seção 6 — P7](#p7--carga-líquida-e-intercâmbio-no-nordeste) |
| **P8** | Quais usinas têm a maior taxa de corte relativa à sua própria geração de referência? | [Seção 6 — P8](#p8--usinas-que-mais-perdem) |

> Fixei estas perguntas **antes** da coleta e não as alterei depois — inclusive a redação da P5, que usa os
> termos que eu tinha antes de ler o dicionário oficial do ONS. Na seção 7 (Autoavaliação) discuto o que consegui
> responder e onde os dados me impuseram limites.

### 1.4 Os dados brutos

Usei cinco datasets do [Portal de Dados Abertos do ONS](https://dados.ons.org.br), que descubro e baixo
programaticamente via API CKAN. Para cada um, usei como referência o **dicionário de dados publicado pelo próprio
ONS**, que informa, campo a campo, tipo, formato e se o valor pode ser nulo, zero ou negativo.

#### Visão geral da estrutura

| Dataset (pacote CKAN) | Tabela Bronze | Periodicidade dos arquivos | Grão | Colunas | Arquivos | Registros | Tamanho |
|---|---|---|---|---|---|---|---|
| `balanco-energia-subsistema` | `balanco_energia_subsistema` | Anual | Subsistema × hora | 9 | 6 | 249.480 | 25,4 MB |
| `ear-diario-por-subsistema` | `ear_diario_subsistema` | Anual | Subsistema × dia | 6 | 6 | 8.320 | 0,5 MB |
| `restricao_coff_eolica_usi` | `restricao_coff_eolica` | Mensal | Usina × 30 min | 24 | 20 | 4.499.184 | 788,8 MB |
| `restricao_coff_fotovoltaica` | `restricao_coff_fotovoltaica` | Mensal | Usina × 30 min | 24 | 20 | 2.061.648 | 331,5 MB |
| `capacidade-geracao` | `capacidade_geracao` | Arquivo único | Unidade geradora | 18 | 1 | 5.678 | 1,2 MB |
| **Total** | | | | | **53** | **6.824.310** | **1.147,4 MB** |

#### `balanco-energia-subsistema` — Balanço de Energia nos Subsistemas

Base horária. É a espinha dorsal do pipeline: traz, na mesma linha, a geração por fonte, a carga e o intercâmbio
de cada subsistema.

| Coluna | Conteúdo |
|---|---|
| `id_subsistema` | Sigla: `N`, `NE`, `SE`, `S` e **`SIN`** — este último é o agregado nacional, e não um subsistema |
| `nom_subsistema` | Nome do subsistema por extenso |
| `din_instante` | Timestamp da hora cheia (`yyyy-MM-dd HH:mm:ss`) |
| `val_gerhidraulica` | Geração hidráulica verificada, MWmed |
| `val_gertermica` | Geração térmica verificada, MWmed |
| `val_gereolica` | Geração eólica verificada, MWmed |
| `val_gersolar` | Geração fotovoltaica verificada, MWmed |
| `val_carga` | Carga verificada, MWmed |
| `val_intercambio` | Intercâmbio líquido, MWmed — o dicionário não define o sinal; confirmei que **positivo = exportação** (seção 5.4) |

#### `restricao_coff_eolica_usi` e `restricao_coff_fotovoltaica` — Constrained-off

Base semi-horária (30 minutos), **por usina ou conjunto de usinas**. São os dois datasets que medem o corte, com
schema idêntico — eu os unifico no pipeline. Durante o projeto, o ONS publicou a **versão 1.5 do dicionário
(08/09/2026)** e republicou os arquivos históricos com oito colunas novas; o pipeline já está ajustado a esse layout.

| Coluna | Conteúdo |
|---|---|
| `id_subsistema`, `nom_subsistema`, `id_estado`, `nom_estado` | Localização da usina |
| `nom_usina`, `id_ons` | Nome e identificador da usina ou do conjunto no ONS |
| `ceg` | Código de Empreendimento de Geração (ANEEL). **Conjuntos de usinas não têm CEG e aparecem como `-`** |
| `din_instante` | Timestamp do intervalo de 30 minutos |
| `val_geracao` | Geração verificada, MWmed (inclui usinas em operação em teste) |
| `val_geracaolimitada` | Limite de geração imposto pelo ONS, MWmed. **Nulo = não houve limitação** |
| `val_disponibilidade` | Disponibilidade eletromecânica das usinas em operação comercial, MWmed |
| `val_geracaoreferencia` | Estimativa do que a usina poderia gerar sem limitação, MWmed |
| `val_geracaoreferenciafinal` | Referência final, **calculada apenas nos períodos com razão REL**, MWmed |
| `cod_razaorestricao` | `REL` (indisponibilidade externa elétrica), `CNF` (requisitos de confiabilidade), `ENE` (razão energética), `PAR` (parecer de acesso) |
| `cod_origemrestricao` | `SIS` (sistêmica) ou `LOC` (local) |
| `dsc_restricao` | Descrição textual do motivo — campo criado em 26/09/2025 |
| `id_pontoconexao`, `nom_pontoconexao` | Ponto de conexão da usina — novo na v1.5 |
| `nom_agenteoperador` | Agente operador — novo na v1.5 |
| `val_geracaonaorealizadaapurada` | **GNRa oficial**: geração frustrada apurada pelo ONS, MWmed — novo na v1.5 |
| `num_minutos_rel`, `num_minutos_cnf`, `num_minutos_ene`, `num_minutos_restricao` | Minutos de restrição no intervalo, por razão — novos na v1.5 |

#### `ear-diario-por-subsistema` — Energia Armazenada Verificada

Base diária. É o "nível dos reservatórios" expresso em energia, e sustenta a P6. Diferentemente do balanço, este
arquivo traz **apenas os quatro subsistemas reais** (não há linha SIN).

| Coluna | Conteúdo |
|---|---|
| `id_subsistema`, `nom_subsistema` | Subsistema |
| `ear_data` | Data de referência |
| `ear_max_subsistema` | Capacidade máxima de armazenamento, MWmês |
| `ear_verif_subsistema_mwmes` | Energia armazenada verificada, MWmês |
| `ear_verif_subsistema_percentual` | Nível de armazenamento, % da capacidade máxima |

#### `capacidade-geracao` — Capacidade Instalada de Geração

Cadastro por unidade geradora das usinas despachadas pelo ONS. Uso para enriquecer a dimensão de usina com a
potência instalada.

| Coluna | Conteúdo |
|---|---|
| `id_subsistema`, `nom_subsistema`, `id_estado`, `nom_estado` | Localização |
| `nom_modalidadeoperacao` | Modalidade de operação (Tipo I, II-A, II-B, II-C) |
| `nom_agenteproprietario`, `nom_agenteoperador` | Agentes da usina |
| `nom_tipousina` | `EOLIELÉTRICA`, `FOTOVOLTAICA`, `HIDROELÉTRICA`, `NUCLEAR`, `TÉRMICA` |
| `nom_usina`, `ceg` | Nome e CEG da usina |
| `nom_unidadegeradora`, `cod_equipamento`, `num_unidadegeradora` | Identificação da unidade geradora |
| `nom_combustivel` | Combustível ou recurso primário |
| `dat_entradateste`, `dat_entradaoperacao`, `dat_desativacao` | Ciclo de vida da unidade (desativação nula = ativa) |
| `val_potenciaefetiva` | Potência nominal da unidade, MW |

### 1.5 Licença de uso

**Os cinco datasets são publicados sob Creative Commons Atribuição (CC-BY)**, conforme o campo `license_id`
devolvido pela própria API CKAN do ONS.

A CC-BY permite uso, redistribuição, adaptação e criação de obras derivadas — inclusive para fins comerciais —
com a única exigência de **atribuição da fonte**. O uso acadêmico deste MVP está integralmente coberto.

A verificação de licença não é uma afirmação minha: ela é **executada** pelo notebook
[`00_setup_ambiente.py`](00_setup_ambiente.py) (seção 4 do notebook), que consulta a API e imprime a licença
declarada de cada dataset no momento da execução.

**Atribuição:** Dados obtidos do Portal de Dados Abertos do Operador Nacional do Sistema Elétrico (ONS) —
https://dados.ons.org.br — sob licença CC-BY.

---

## 2. Carga dos Dados (Etapa 4.2)

### 2.1 Estratégia: coleta por API, não upload manual

O enunciado admite o upload manual de arquivos. Optei pela **coleta programática via API**, por três razões:

1. **Reprodutibilidade.** O pipeline roda do zero em um workspace vazio. Quem clonar este repositório executa e
   obtém o mesmo resultado.
2. **Volume.** São 53 arquivos e 1,15 GB. O upload manual seria trabalhoso e propenso a erro de seleção.
3. **Atualização.** Como descubro os arquivos a cada execução, o pipeline captura automaticamente os meses novos
   que o ONS publicar, sem alteração de código.

**Scripts:** [`_config.py`](_config.py) (fontes, janela e funções de acesso à API),
[`00_setup_ambiente.py`](00_setup_ambiente.py) (catálogo, schemas, Volume e licenças) e
[`01_coleta_bronze.py`](01_coleta_bronze.py) (coleta e camada Bronze).

### 2.2 Fluxo implementado

```
   API CKAN do ONS                Volume do Unity Catalog              Tabela Delta
   package_show?id=...    ──►     /Volumes/mvp_ons/bronze/     ──►     mvp_ons.bronze.*
   (descoberta das URLs)          raw_ons/<dataset>/*.csv              (todas as colunas STRING)
                                  (arquivo preservado)
```

**Por que descubro as URLs pela API.** O ONS publica os arquivos no S3 sob caminhos que **não correspondem** ao
nome do pacote CKAN — o pacote `restricao_coff_eolica_usi` publica em `.../dataset/restricao_coff_eolica_tm/`.
Uma URL montada por convenção quebraria em silêncio. Por isso minha função `listar_recursos_csv()` consulta
`package_show` e usa exatamente a URL que a API devolve.

**Por que aterrisso no Volume antes de criar a tabela.** Porque rastreabilidade é o propósito da camada Bronze.
Preservando o arquivo exatamente como o ONS publicou, consigo auditar depois o que chegou — o que importa neste
domínio, já que o ONS revisa e republica dados históricos (aconteceu durante este projeto).

**Idempotência.** Programei o download para pular arquivos já presentes no Volume. Uma falha de rede no arquivo
40 de 53 não me obriga a baixar de novo os 39 anteriores.

**Manifesto de linhagem.** Cada download alimenta a tabela `bronze._manifesto_ingestao`, onde registro, para cada
arquivo, o dataset, a **URL exata de onde veio**, o caminho no Volume, o tamanho e o instante da coleta.

### 2.3 Leitura para a Bronze

Leio os CSVs com `sep=';'`, `encoding='UTF-8'`, `inferSchema=false` (tudo `STRING`) e **`multiLine=true`**, e
acrescento três colunas de controle: `_ingest_ts`, `_source_file` e `_source_dataset`.

### 2.4 O que foi coletado

| Dataset | Janela | Arquivos | Registros | Tamanho |
|---|---|---|---|---|
| Balanço de energia | 2021 a 2026 (até 10/09/2026) | 6 | 249.480 | 25,4 MB |
| EAR diário | 2021 a 2026 (até 11/09/2026) | 6 | 8.320 | 0,5 MB |
| Constrained-off eólica | jan/2025 a ago/2026 | 20 | 4.499.184 | 788,8 MB |
| Constrained-off solar | jan/2025 a ago/2026 | 20 | 2.061.648 | 331,5 MB |
| Capacidade instalada | Cadastro atual | 1 | 5.678 | 1,2 MB |

Escolhi janelas diferentes de propósito. Balanço e EAR são leves e cobrem seis anos, dando profundidade histórica
a P1, P2 e P7. Os arquivos de constrained-off são mensais e pesados, então cobri 20 meses. Deixei as janelas como
parâmetros em [`_config.py`](_config.py) (`ANO_INICIO`, `ANO_FIM`, `COFF_INICIO`, `COFF_FIM`).

### 2.5 Três problemas que encontrei na coleta

1. **Registros com quebra de linha dentro de campo texto.** O ONS publica `dsc_restricao` e `nom_pontoconexao`
   entre aspas, às vezes com quebra de linha dentro do texto. Sem `multiLine`, o Spark partia **2.280 registros
   da eólica e 622 da solar** em fragmentos inválidos, que eram descartados na Silver. Com `multiLine=true`,
   todos são lidos inteiros.
2. **Restrição de rede da Databricks Free Edition.** A Free Edition limita o acesso de saída à internet a
   domínios confiáveis, e os domínios do ONS não estão na lista por padrão. A Databricks libera o acesso na
   verificação da conta pelo LinkedIn. O notebook `01` começa com um **teste de conectividade** e documenta um
   **plano B** (upload dos mesmos arquivos para o Volume), que funciona sem alterar nenhuma etapa seguinte.
3. **Mudança de layout na fonte.** Em 08/09/2026 o ONS republicou os arquivos de restrição com 24 colunas em vez
   de 16. O volume dobrou em relação ao que eu tinha estimado (1,15 GB em vez de ~640 MB), e passei a aproveitar
   as colunas novas — em especial a GNRa oficial, que uso para validar meu cálculo de corte.

### 2.6 Evidências na plataforma

![Volume raw_ons com as subpastas por dataset](docs/img/databricks/01-volume-raw-ons.png)

![Saída do download no notebook 01](docs/img/databricks/02-coleta-download.png)

![Tabela bronze._manifesto_ingestao](docs/img/databricks/03-manifesto-ingestao.png)

![Verificação de licença CC-BY no notebook 00](docs/img/databricks/04-verificacao-licenca.png)

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

### 3.1 O modelo que escolhi, e por quê

**Constelação de fatos** — várias tabelas fato compartilhando dimensões conformes.

A escolha me foi imposta pelos dados. As perguntas vivem em **três granularidades diferentes**:

| Perguntas | Granularidade natural |
|---|---|
| P1, P2, P7 | Subsistema × hora |
| P3, P4, P5, P8 | Usina × 30 minutos |
| P6 | Subsistema × dia |

Se eu forçasse tudo em um fato único, teria de explodir o grão diário em horário (inventando dados) ou agregar o
dado de usina até perder o detalhe de que P8 depende. Na constelação, cada fato guarda o próprio grão e as
dimensões conformes garantem que eles conversem.

```
                         dim_data ───────────┬─────────── dim_hora
                             │               │                │
       ┌─────────────────────┼───────────────┼────────────────┼───────────────────┐
       │                     │               │                │                   │
 fato_reservatorio     fato_balanco     fato_geracao     fato_restricao           │
     _diario             _horario         _horaria          _horaria              │
       │                     │               │                │     │             │
       └────────────── dim_subsistema ───────┴────────────────┘     │             │
                             │                                      │             │
                             └──────── dim_fonte_geracao ───────────┘         dim_usina
```

**Por que separei `dim_data` de `dim_hora`.** Uma dimensão de tempo horária única não serviria ao fato diário de
reservatório. Separando data (grão dia) de hora do dia (24 linhas), os quatro fatos convivem: os horários usam as
duas chaves e o diário usa apenas `sk_data`.

**A transformação estrutural: UNPIVOT.** O balanço do ONS chega em formato largo — uma coluna por fonte. Em
`fato_geracao_horaria` eu despivoto as quatro colunas em quatro linhas, o que transforma "fonte de geração" de
*coluna* em *dimensão*. Já `fato_balanco_horario` fica em formato largo de propósito: carga, intercâmbio e carga
líquida não são fontes de geração.

### 3.2 Tabelas persistidas

| Camada | Tabela | Grão | Linhas |
|---|---|---|---|
| Bronze | `balanco_energia_subsistema` | Como veio da fonte | 249.480 |
| Bronze | `ear_diario_subsistema` | Como veio da fonte | 8.320 |
| Bronze | `restricao_coff_eolica` | Como veio da fonte | 4.499.184 |
| Bronze | `restricao_coff_fotovoltaica` | Como veio da fonte | 2.061.648 |
| Bronze | `capacidade_geracao` | Como veio da fonte | 5.678 |
| Bronze | `_manifesto_ingestao` | Arquivo coletado | 53 |
| Silver | `balanco_subsistema_horario` | Subsistema × hora (inclui SIN marcado) | 249.480 |
| Silver | `ear_subsistema_diario` | Subsistema × dia | 8.320 |
| Silver | `restricao_coff_usina` | Usina × 30 min | 6.560.832 |
| Silver | `capacidade_instalada_ug` | Unidade geradora | 5.678 |
| Gold | `dim_data` | Dia (01/01/2021 a 11/09/2026) | 2.080 |
| Gold | `dim_hora` | Hora do dia | 24 |
| Gold | `dim_subsistema` | Subsistema | 5 |
| Gold | `dim_fonte_geracao` | Fonte | 4 |
| Gold | `dim_usina` | Usina ou conjunto | 248 |
| Gold | `fato_geracao_horaria` | Subsistema × hora × fonte | 798.336 |
| Gold | `fato_balanco_horario` | Subsistema × hora | 199.584 |
| Gold | `fato_restricao_horaria` | Usina × hora | 3.280.416 |
| Gold | `fato_reservatorio_diario` | Subsistema × dia | 8.320 |
| Gold | `qualidade_perfil_atributos` | Atributo perfilado | 81 |

### 3.3 Catálogo de dados

O catálogo abaixo descreve **cada tabela e cada campo** do modelo: tipo, descrição, domínio de valores e linhagem.
Ele não é um documento paralelo ao código: as mesmas descrições são gravadas no **Unity Catalog** pelo próprio
pipeline, com `COMMENT ON` e `ALTER TABLE ... ALTER COLUMN ... COMMENT`, e ficam visíveis no Catalog Explorer.
Os domínios observados vêm da minha execução.

**Convenções:** `sk_` = chave substituta · `id_` = chave natural da fonte · `_mwmed` = potência média no
intervalo (MW médios) · `_mwh` = energia (MWh) · `_mwmes` = energia armazenada (MW-mês) · `eh_` = booleano.
**Aditividade:** métricas *aditivas* podem ser somadas em qualquer dimensão; *semi-aditivas* somam entre
subsistemas mas não ao longo do tempo; *não aditivas* (razões e percentuais) nunca devem ser somadas.

#### 3.3.1 Camada Bronze — `mvp_ons.bronze`

Regra da camada: dados exatamente como publicados pelo ONS. **Todas as colunas de negócio são `STRING`.** Toda
tabela Bronze tem as colunas de controle `_ingest_ts` (TIMESTAMP, instante da carga), `_source_file` (STRING,
caminho do arquivo no Volume) e `_source_dataset` (STRING, chave do dataset).

**`bronze.balanco_energia_subsistema`** — Balanço de energia por subsistema em base horária, cru. Linhagem:
arquivos anuais `BALANCO_ENERGIA_SUBSISTEMA_<ano>.csv` do dataset `balanco-energia-subsistema`.

| Campo | Tipo | Descrição | Domínio esperado |
|---|---|---|---|
| `id_subsistema` | STRING | Sigla do subsistema | `N`, `NE`, `SE`, `S`, `SIN` |
| `nom_subsistema` | STRING | Nome do subsistema | Texto em caixa alta |
| `din_instante` | STRING | Hora de referência | `yyyy-MM-dd HH:mm:ss` |
| `val_gerhidraulica` | STRING | Geração hidráulica, MWmed | Numérico ≥ 0 |
| `val_gertermica` | STRING | Geração térmica, MWmed | Numérico ≥ 0 |
| `val_gereolica` | STRING | Geração eólica, MWmed | Numérico ≥ 0 |
| `val_gersolar` | STRING | Geração fotovoltaica, MWmed | Numérico ≥ 0 |
| `val_carga` | STRING | Carga verificada, MWmed | Numérico ≥ 0 |
| `val_intercambio` | STRING | Intercâmbio líquido, MWmed | Numérico, pode ser negativo |

**`bronze.ear_diario_subsistema`** — Energia armazenada verificada por subsistema, cru. Linhagem: arquivos anuais
`EAR_DIARIO_SUBSISTEMA_<ano>.csv` do dataset `ear-diario-por-subsistema`.

| Campo | Tipo | Descrição | Domínio esperado |
|---|---|---|---|
| `id_subsistema` | STRING | Sigla do subsistema | `N`, `NE`, `SE`, `S` |
| `nom_subsistema` | STRING | Nome do subsistema | Texto |
| `ear_data` | STRING | Data de referência | `yyyy-MM-dd` |
| `ear_max_subsistema` | STRING | EAR máxima, MWmês | Numérico > 0 |
| `ear_verif_subsistema_mwmes` | STRING | EAR verificada, MWmês | Numérico > 0 |
| `ear_verif_subsistema_percentual` | STRING | EAR verificada, % da máxima | 0 a 100 |

**`bronze.restricao_coff_eolica`** e **`bronze.restricao_coff_fotovoltaica`** — Restrição por constrained-off de
usinas eólicas e fotovoltaicas, cru, com schema idêntico. Linhagem: arquivos mensais
`RESTRICAO_COFF_EOLICA_<ano>_<mes>.csv` e `RESTRICAO_COFF_FOTOVOLTAICA_<ano>_<mes>.csv`.

| Campo | Tipo | Descrição | Domínio esperado |
|---|---|---|---|
| `id_subsistema`, `nom_subsistema` | STRING | Subsistema da usina | `N`, `NE`, `SE`, `S` |
| `id_estado`, `nom_estado` | STRING | UF da usina | Siglas e nomes de UF |
| `nom_usina` | STRING | Usina ou conjunto (`Conj. ...`) | Texto |
| `id_ons` | STRING | Identificador no ONS | Texto, até 32 posições |
| `ceg` | STRING | CEG da ANEEL | Código ou `-` para conjuntos |
| `din_instante` | STRING | Início do intervalo de 30 min | `yyyy-MM-dd HH:mm:ss` |
| `val_geracao` | STRING | Geração verificada, MWmed | Numérico ≥ 0 |
| `val_geracaolimitada` | STRING | Limite imposto, MWmed | Numérico ≥ 0; vazio = sem limitação |
| `val_disponibilidade` | STRING | Disponibilidade, MWmed | Numérico ≥ 0 |
| `val_geracaoreferencia` | STRING | Geração de referência, MWmed | Numérico ≥ 0 |
| `val_geracaoreferenciafinal` | STRING | Referência final, MWmed | Numérico ≥ 0; só em intervalos REL |
| `cod_razaorestricao` | STRING | Razão da restrição | `REL`, `CNF`, `ENE`, `PAR`, vazio |
| `cod_origemrestricao` | STRING | Origem da restrição | `SIS`, `LOC`, vazio |
| `dsc_restricao` | STRING | Descrição do motivo | Texto até 600 posições |
| `id_pontoconexao`, `nom_pontoconexao` | STRING | Ponto de conexão | Texto |
| `nom_agenteoperador` | STRING | Agente operador | Texto |
| `val_geracaonaorealizadaapurada` | STRING | GNRa oficial, MWmed | Numérico ≥ 0 |
| `num_minutos_rel`, `num_minutos_cnf`, `num_minutos_ene`, `num_minutos_restricao` | STRING | Minutos de restrição no intervalo | Inteiro 0 a 30 |

**`bronze.capacidade_geracao`** — Capacidade instalada por unidade geradora, cru. Linhagem:
`CAPACIDADE_GERACAO.csv` do dataset `capacidade-geracao`.

| Campo | Tipo | Descrição | Domínio esperado |
|---|---|---|---|
| `id_subsistema`, `nom_subsistema` | STRING | Subsistema (nome com espaços de preenchimento) | `N`, `NE`, `SE`, `S` |
| `id_estado`, `nom_estado` | STRING | UF | Siglas e nomes de UF |
| `nom_modalidadeoperacao` | STRING | Modalidade de operação | Tipo I, II-A, II-B, II-C |
| `nom_agenteproprietario`, `nom_agenteoperador` | STRING | Agentes | Texto |
| `nom_tipousina` | STRING | Tipo da usina | `EOLIELÉTRICA`, `FOTOVOLTAICA`, `HIDROELÉTRICA`, `NUCLEAR`, `TÉRMICA` |
| `nom_usina` | STRING | Nome da usina | Texto em caixa alta |
| `ceg` | STRING | CEG da ANEEL | Código |
| `nom_unidadegeradora` | STRING | Nome da unidade geradora | Texto |
| `cod_equipamento` | STRING | Código da unidade (com espaços de preenchimento) | Texto |
| `num_unidadegeradora` | STRING | Código operacional (com espaços de preenchimento) | Texto |
| `nom_combustivel` | STRING | Combustível ou recurso | Texto |
| `dat_entradateste`, `dat_entradaoperacao`, `dat_desativacao` | STRING | Datas do ciclo de vida | `yyyy-MM-dd`; desativação vazia = ativa |
| `val_potenciaefetiva` | STRING | Potência nominal da unidade, MW | Numérico > 0 |

**`bronze._manifesto_ingestao`** — Linhagem da coleta: uma linha por arquivo aterrissado no Volume. Linhagem:
gerado pelo notebook `01` durante o download.

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| `dataset` | STRING | Chave do dataset | Cinco valores (datasets do projeto) |
| `arquivo` | STRING | Nome do arquivo CSV | Texto |
| `url_origem` | STRING | URL exata devolvida pela API CKAN | URL do S3 do ONS |
| `caminho_volume` | STRING | Caminho no Volume | `/Volumes/mvp_ons/bronze/raw_ons/...` |
| `bytes` | BIGINT | Tamanho do arquivo | > 0 |
| `status` | STRING | Resultado da coleta | `cache` ou `baixado em Ns` |
| `ingest_ts` | STRING | Instante da coleta | ISO-8601 |
| `_ingest_ts` | TIMESTAMP | Instante do registro | — |

#### 3.3.2 Camada Silver — `mvp_ons.silver`

Regra da camada: dados limpos, tipados, deduplicados e padronizados. Todo texto passou por `TRIM`; sentinelas
(`''`, `'-'`, `'N/A'`, `'NULL'`) viraram `NULL`; números e datas foram convertidos.

**`silver.balanco_subsistema_horario`** — Grão: subsistema × hora. Linhagem: `bronze.balanco_energia_subsistema`.

| Campo | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| `id_subsistema` | STRING | Sigla — `N`, `NE`, `SE`, `S`, `SIN` | TRIM de `id_subsistema` |
| `nom_subsistema_origem` | STRING | Nome como publicado | TRIM de `nom_subsistema` |
| `din_instante` | TIMESTAMP | Hora cheia — 01/01/2021 00h a 10/09/2026 23h | CAST de `din_instante` |
| `ger_hidraulica_mwmed` | DOUBLE | Geração hidráulica — 0 a 49.700 MWmed | CAST de `val_gerhidraulica` |
| `ger_termica_mwmed` | DOUBLE | Geração térmica — 0 a 12.525 MWmed | CAST de `val_gertermica` |
| `ger_eolica_mwmed` | DOUBLE | Geração eólica — 0 a 22.681 MWmed | CAST de `val_gereolica` |
| `ger_solar_mwmed` | DOUBLE | Geração solar — 0 a 24.531 MWmed | CAST de `val_gersolar` |
| `carga_mwmed` | DOUBLE | Carga — 1.364 a 62.150 MWmed por subsistema | CAST de `val_carga` |
| `intercambio_mwmed` | DOUBLE | Intercâmbio líquido, positivo = exportação — −16.172 a 15.300 MWmed | CAST de `val_intercambio` |
| `eh_agregado` | BOOLEAN | **Verdadeiro só para `SIN`** (total nacional) | Derivado: `id_subsistema = 'SIN'` |
| `ger_total_mwmed` | DOUBLE | Soma das quatro fontes, MWmed | Derivado |
| `ger_intermitente_mwmed` | DOUBLE | Eólica + solar, MWmed | Derivado |
| `carga_liquida_mwmed` | DOUBLE | Carga − intermitente — −12.896 a 60.167 MWmed | Derivado |
| `pct_intermitente_sobre_carga` | DOUBLE | Intermitente ÷ carga × 100 — 0 a 206,3 | Derivado |
| `_source_file`, `_ingest_ts` | STRING, TIMESTAMP | Controle | Propagados da Bronze |

**`silver.ear_subsistema_diario`** — Grão: subsistema × dia. Linhagem: `bronze.ear_diario_subsistema`.

| Campo | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| `id_subsistema` | STRING | `N`, `NE`, `SE`, `S` | TRIM |
| `dat_referencia` | DATE | 01/01/2021 a 11/09/2026 | CAST de `ear_data` |
| `ear_maxima_mwmes` | DOUBLE | Capacidade máxima — 15.302 a 204.615 MWmês | CAST de `ear_max_subsistema` |
| `ear_verificada_mwmes` | DOUBLE | Armazenamento — 4.377 a 177.379 MWmês | CAST de `ear_verif_subsistema_mwmes` |
| `ear_percentual` | DOUBLE | Nível — 16,5% a 99,8% | CAST de `ear_verif_subsistema_percentual` |
| `eh_agregado` | BOOLEAN | Marcação de `SIN` (sempre falso neste arquivo) | Derivado |
| `_source_file`, `_ingest_ts` | STRING, TIMESTAMP | Controle | Propagados |

**`silver.restricao_coff_usina`** — Grão: usina × 30 minutos. Linhagem: `UNION` de `bronze.restricao_coff_eolica`
e `bronze.restricao_coff_fotovoltaica`.

| Campo | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| `id_subsistema`, `id_estado`, `nom_estado` | STRING | Localização — NE, SE, S, N | TRIM |
| `nom_usina` | STRING | Usina ou conjunto — 248 entradas | TRIM |
| `id_ons` | STRING | Identificador da usina no ONS | TRIM |
| `ceg` | STRING | CEG; **NULL para os 233 conjuntos** | `'-'` → NULL |
| `din_instante` | TIMESTAMP | 01/01/2025 00h00 a 31/08/2026 23h30 | CAST |
| `geracao_mwmed` | DOUBLE | Geração verificada — −1,4 a 1.320,6 MWmed | CAST de `val_geracao` |
| `geracao_limitada_mwmed` | DOUBLE | Limite imposto; NULL = sem limitação | CAST |
| `disponibilidade_mwmed` | DOUBLE | Disponibilidade, MWmed | CAST |
| `geracao_referencia_mwmed` | DOUBLE | Referência — 0 a 1.327,9 MWmed | CAST |
| `geracao_referencia_final_mwmed` | DOUBLE | Referência final; só intervalos REL | CAST |
| `cod_razao_restricao` | STRING | `REL`, `CNF`, `ENE`, `PAR`, NULL | TRIM + sentinela |
| `cod_origem_restricao` | STRING | `SIS`, `LOC`, NULL | TRIM + sentinela |
| `dsc_restricao` | STRING | Descrição do motivo (a partir de set/2025) | TRIM + sentinela |
| `id_ponto_conexao`, `nom_ponto_conexao`, `nom_agente_operador` | STRING | Conexão e agente | TRIM |
| `gnra_oficial_mwmed` | DOUBLE | GNRa oficial do ONS, MWmed | CAST de `val_geracaonaorealizadaapurada` |
| `num_minutos_rel`, `num_minutos_cnf`, `num_minutos_ene`, `num_minutos_restricao` | INT | Minutos de restrição — 0 a 30 | CAST |
| `cod_fonte`, `nom_fonte` | STRING | `EOL`/Eólica ou `SOL`/Solar | Derivado da tabela de origem |
| `houve_restricao` | BOOLEAN | Há razão de restrição declarada | Derivado |
| `corte_mwmed` | DOUBLE | `GREATEST(referência − geração, 0)` se há restrição | Derivado |
| `corte_mwh` | DOUBLE | Energia cortada no intervalo — 0 a 618 MWh | `corte_mwmed × 0,5` |
| `geracao_mwh`, `geracao_referencia_mwh`, `gnra_oficial_mwh` | DOUBLE | Energias do intervalo, MWh | `× 0,5` |
| `dsc_razao_restricao` | STRING | Rótulo oficial da razão | Derivado de `cod_razao_restricao` |
| `dsc_origem_restricao` | STRING | Sistêmica ou Local | Derivado de `cod_origem_restricao` |
| `_source_file`, `_ingest_ts` | STRING, TIMESTAMP | Controle | Propagados |

**`silver.capacidade_instalada_ug`** — Grão: unidade geradora. Linhagem: `bronze.capacidade_geracao`.

| Campo | Tipo | Descrição e domínio | Linhagem |
|---|---|---|---|
| `id_subsistema`, `id_estado`, `nom_estado` | STRING | Localização | TRIM |
| `nom_tipo_usina` | STRING | Cinco tipos de usina | TRIM de `nom_tipousina` |
| `nom_usina` | STRING | Nome da usina — 2.043 nomes distintos | TRIM |
| `ceg` | STRING | CEG — **chave do JOIN com a restrição** | TRIM |
| `nom_combustivel`, `nom_agente_proprietario` | STRING | Combustível e proprietário | TRIM |
| `cod_equipamento` | STRING | Código da unidade geradora | TRIM |
| `dat_entrada_operacao` | DATE | 1924 a 2026 | CAST |
| `dat_desativacao` | DATE | NULL = ativa | CAST |
| `potencia_efetiva_mw` | DOUBLE | Potência da unidade — 0,3 a 1.350 MW | CAST de `val_potenciaefetiva` |
| `esta_ativa` | BOOLEAN | `dat_desativacao IS NULL` | Derivado |
| `_ingest_ts` | TIMESTAMP | Controle | Propagado |

#### 3.3.3 Camada Gold — dimensões (`mvp_ons.gold`)

**`gold.dim_data`** — Calendário no grão dia, cobrindo as datas de **todos** os fatos. Linhagem: gerada pelo
pipeline a partir do intervalo de datas das tabelas Silver de balanço, EAR e restrição.

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `sk_data` | INT | **PK.** `AAAAMMDD` — 20210101 a 20260911 |
| `data` | DATE | Data civil |
| `ano` | INT | 2021 a 2026 |
| `mes` | INT | 1 a 12 |
| `nom_mes` | STRING | Janeiro a Dezembro |
| `dia` | INT | 1 a 31 |
| `ano_mes` | STRING | `AAAA-MM` |
| `trimestre` | INT | 1 a 4 |
| `num_dia_semana` | INT | 1 (domingo) a 7 (sábado) |
| `nom_dia_semana` | STRING | Domingo a Sábado |
| `eh_fim_semana` | BOOLEAN | Sábado ou domingo |
| `dia_do_ano` | INT | 1 a 366 |
| `estacao_ano` | STRING | Verão, Outono, Inverno, Primavera (hemisfério sul) |

**`gold.dim_hora`** — Hora do dia. Linhagem: gerada pelo pipeline.

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `sk_hora` | INT | **PK.** 0 a 23 |
| `hora` | INT | 0 a 23 |
| `hora_rotulo` | STRING | `00h` a `23h` |
| `periodo_dia` | STRING | Madrugada (0–5), Manhã (6–11), Tarde (12–17), Noite (18–23) |
| `eh_horario_ponta` | BOOLEAN | 18h a 20h |
| `eh_janela_solar` | BOOLEAN | 6h a 18h |

**`gold.dim_subsistema`** — Subsistemas do SIN. Linhagem: definida no pipeline (`_config`) a partir da definição
operativa do ONS.

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `sk_subsistema` | STRING | **PK.** `N`, `NE`, `SE`, `S`, `SIN` |
| `id_subsistema` | STRING | Sigla do ONS |
| `nom_subsistema` | STRING | Norte, Nordeste, Sudeste/C.O., Sul, Brasil |
| `nom_subsistema_ons` | STRING | Nome exatamente como o ONS publica |
| `estados` | STRING | UFs abrangidas |
| `eh_agregado` | BOOLEAN | **Verdadeiro só para `SIN`** — filtrar ao somar |

**`gold.dim_fonte_geracao`** — Fontes de geração. Linhagem: definida no pipeline (`_config`).

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `sk_fonte` | STRING | **PK.** `HID`, `TER`, `EOL`, `SOL` |
| `cod_fonte` | STRING | Mesmo domínio |
| `nom_fonte` | STRING | Hidráulica, Térmica, Eólica, Solar |
| `eh_renovavel` | BOOLEAN | Hidráulica, eólica e solar |
| `eh_despachavel` | BOOLEAN | Hidráulica e térmica |
| `eh_intermitente` | BOOLEAN | Eólica e solar |
| `coluna_origem` | STRING | Coluna do balanço que originou a fonte no UNPIVOT |

**`gold.dim_usina`** — Usinas e conjuntos com registro de constrained-off (248: 163 eólicas e 85 solares).
Linhagem: `silver.restricao_coff_usina` (atributos do registro mais recente de cada `id_ons`) com `LEFT JOIN` em
`silver.capacidade_instalada_ug` pelo **CEG**.

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `sk_usina` | STRING | **PK.** `id_ons` |
| `id_ons` | STRING | Identificador no ONS |
| `nom_usina` | STRING | Nome; prefixo `Conj.` = conjunto de usinas (233 de 248) |
| `ceg` | STRING | CEG; NULL para conjuntos |
| `cod_fonte`, `nom_fonte` | STRING | `EOL`/Eólica (163) ou `SOL`/Solar (85) |
| `id_estado`, `nom_estado` | STRING | UF |
| `id_subsistema` | STRING | NE (205), SE (29), S (13), N (1) |
| `potencia_instalada_mw` | DOUBLE | Soma das unidades ativas com o mesmo CEG; **NULL para os 233 conjuntos** |
| `qtd_unidades_geradoras` | BIGINT | Unidades somadas |
| `dat_entrada_operacao` | DATE | Unidade mais antiga |
| `disponibilidade_maxima_mwmed` | DOUBLE | Maior disponibilidade observada |
| `tem_cadastro_capacidade` | BOOLEAN | JOIN encontrou correspondência (15 de 248) |
| `primeiro_registro`, `ultimo_registro` | TIMESTAMP | Período observado da usina |

#### 3.3.4 Camada Gold — fatos

**`gold.fato_geracao_horaria`** — Grão: subsistema × hora × fonte. Linhagem: UNPIVOT de
`silver.balanco_subsistema_horario`, sem a linha SIN.

| Campo | Tipo | Aditividade | Descrição e domínio |
|---|---|---|---|
| `sk_data` | INT | — | FK → `dim_data` |
| `sk_hora` | INT | — | FK → `dim_hora` |
| `sk_subsistema` | STRING | — | FK → `dim_subsistema` — N, NE, SE, S |
| `sk_fonte` | STRING | — | FK → `dim_fonte_geracao` — HID, TER, EOL, SOL |
| `din_instante` | TIMESTAMP | — | Hora cheia |
| `geracao_mwmed` | DOUBLE | Aditiva | Geração média na hora, ≥ 0 |
| `geracao_mwh` | DOUBLE | Aditiva | Energia na hora (igual a MWmed, intervalo de 1 h) |

**`gold.fato_balanco_horario`** — Grão: subsistema × hora. Linhagem: `silver.balanco_subsistema_horario`, sem SIN.

| Campo | Tipo | Aditividade | Descrição e domínio |
|---|---|---|---|
| `sk_data`, `sk_hora`, `sk_subsistema` | INT, INT, STRING | — | FKs |
| `din_instante` | TIMESTAMP | — | Hora cheia |
| `carga_mwmed` | DOUBLE | Aditiva | Carga do subsistema |
| `ger_total_mwmed` | DOUBLE | Aditiva | Geração das quatro fontes |
| `ger_intermitente_mwmed` | DOUBLE | Aditiva | Eólica + solar |
| `carga_liquida_mwmed` | DOUBLE | Aditiva | Carga − intermitente; pode ser negativa |
| `intercambio_mwmed` | DOUBLE | Aditiva | Positivo = exportação |
| `pct_intermitente_sobre_carga` | DOUBLE | Não aditiva | Intermitente ÷ carga × 100 |
| `carga_liquida_negativa` | BOOLEAN | — | Intermitente superou a carga na hora |

**`gold.fato_restricao_horaria`** — Grão: usina × hora. Linhagem: agregação de `silver.restricao_coff_usina`
(dois intervalos de 30 min por hora), somando energia e separando o corte por razão e por origem.

| Campo | Tipo | Aditividade | Descrição e domínio |
|---|---|---|---|
| `sk_data`, `sk_hora` | INT | — | FKs → `dim_data`, `dim_hora` |
| `sk_usina` | STRING | — | FK → `dim_usina` |
| `sk_subsistema` | STRING | — | FK → `dim_subsistema` |
| `sk_fonte` | STRING | — | FK → `dim_fonte_geracao` — EOL, SOL |
| `din_instante` | TIMESTAMP | — | Hora cheia |
| `corte_mwh` | DOUBLE | Aditiva | Energia cortada na hora — 0 a 1.231,8 MWh |
| `corte_rel_mwh`, `corte_cnf_mwh`, `corte_ene_mwh`, `corte_par_mwh` | DOUBLE | Aditiva | Parcela do corte por razão |
| `corte_sis_mwh`, `corte_loc_mwh` | DOUBLE | Aditiva | Parcela do corte por origem |
| `gnra_oficial_mwh` | DOUBLE | Aditiva | GNRa oficial do ONS, para validação |
| `geracao_mwh` | DOUBLE | Aditiva | Energia gerada |
| `geracao_referencia_mwh` | DOUBLE | Aditiva | Energia que poderia ter sido gerada |
| `intervalos_com_restricao` | BIGINT | Aditiva | 0 a 2 |
| `intervalos_no_periodo` | BIGINT | Aditiva | Sempre 2 no período coletado |
| `cod_razao_predominante` | STRING | — | Razão mais frequente na hora (só leitura) |
| `houve_restricao_na_hora` | BOOLEAN | — | Algum intervalo restrito |
| `taxa_corte_pct` | DOUBLE | Não aditiva | Corte ÷ referência × 100 — 0 a 100 |

**`gold.fato_reservatorio_diario`** — Grão: subsistema × dia. Linhagem: `silver.ear_subsistema_diario`.

| Campo | Tipo | Aditividade | Descrição e domínio |
|---|---|---|---|
| `sk_data` | INT | — | FK → `dim_data` |
| `sk_subsistema` | STRING | — | FK → `dim_subsistema` — N, NE, SE, S |
| `dat_referencia` | DATE | — | Data da medição |
| `ear_verificada_mwmes` | DOUBLE | Semi-aditiva | Estoque: soma entre subsistemas, não no tempo |
| `ear_maxima_mwmes` | DOUBLE | Semi-aditiva | Capacidade máxima |
| `ear_percentual` | DOUBLE | Não aditiva | 16,5% a 99,8% |
| `faixa_armazenamento` | STRING | — | Crítico (<30%), Baixo (30–50%), Confortável (50–70%), Alto (>70%) |

**`gold.qualidade_perfil_atributos`** — Perfil de completude de todos os atributos da Bronze. Linhagem: notebook
`04` sobre as cinco tabelas Bronze.

| Campo | Tipo | Descrição e domínio |
|---|---|---|
| `tabela`, `coluna` | STRING | Atributo perfilado — 81 atributos |
| `linhas_totais` | BIGINT | Linhas da tabela |
| `qtd_nulos`, `qtd_sentinelas` | BIGINT | Ausências por tipo |
| `pct_ausente` | DOUBLE | 0 a 100 |
| `qtd_valores_distintos` | BIGINT | Cardinalidade aproximada |
| `_perfilado_em` | TIMESTAMP | Instante da perfilagem |

### 3.4 Chaves e integridade referencial

No notebook `03` declaro **chaves primárias** nas cinco dimensões e **14 chaves estrangeiras** dos fatos para as
dimensões. No Unity Catalog essas constraints são informativas — não são impostas na escrita —, mas alimentam o
diagrama de relacionamento e o otimizador. Por isso também **verifico a integridade por consulta**, com
`LEFT ANTI JOIN` de cada chave estrangeira contra a dimensão.

Essa verificação encontrou um problema real na primeira execução: **4 chaves órfãs** em
`fato_reservatorio_diario → dim_data`. O EAR é publicado com um dia a mais que o balanço (11/09 contra 10/09), e
eu construía a `dim_data` só com as datas do balanço. Corrigi a dimensão para cobrir as datas de todos os fatos, e
a verificação passou a retornar **zero órfãs** em todas as relações.

### 3.5 Evidências na plataforma

![Catálogo mvp_ons com os três schemas](docs/img/databricks/05-catalogo-schemas.png)

![Descrições de coluna de gold.fato_restricao_horaria no Catalog Explorer](docs/img/databricks/06-catalogo-colunas.png)

![Aba Lineage de uma tabela Gold](docs/img/databricks/07-lineage.png)

![Constraints PK e FK declaradas](docs/img/databricks/08-constraints-pk-fk.png)

![Verificação de integridade referencial com zero órfãs](docs/img/databricks/09-integridade-referencial.png)

---

## 4. Pipeline de Dados (Etapa 4.4)

### 4.1 Como organizei: um notebook por etapa

Ramifiquei o pipeline em **seis notebooks**, mais um de configuração compartilhada. Descartei a alternativa de um
notebook único porque a coleta leva vários minutos, e eu teria de reexecutá-la a cada ajuste em uma transformação
da Gold.

| Notebook | O que faz | Lê de | Grava em |
|---|---|---|---|
| [`_config.py`](_config.py) | Nomes de catálogo e schemas, janela temporal, fontes, funções de API, desligamento do modo ANSI e `esc_sql()` | — | — |
| [`00_setup_ambiente.py`](00_setup_ambiente.py) | Cria catálogo, schemas e Volume; verifica licenças | API CKAN | Unity Catalog |
| [`01_coleta_bronze.py`](01_coleta_bronze.py) | Testa conectividade, descobre e baixa os arquivos, cria a Bronze | API CKAN / S3 do ONS | Volume, `bronze.*` |
| [`02_silver_limpeza.py`](02_silver_limpeza.py) | Limpeza, tipagem, deduplicação, união das restrições, cálculo do corte | `bronze.*` | `silver.*` |
| [`03_gold_modelagem.py`](03_gold_modelagem.py) | Dimensões, fatos, catálogo de dados, PK/FK e integridade | `silver.*` | `gold.*` |
| [`04_qualidade_dados.py`](04_qualidade_dados.py) | Perfilagem e verificações de qualidade | `bronze.*`, `silver.*` | `gold.qualidade_perfil_atributos` |
| [`05_analise_perguntas.py`](05_analise_perguntas.py) | Responde P1 a P8 com consultas, gráficos e discussão | `gold.*` | — |

Criei o notebook `_config` para resolver um problema banal e recorrente: o mesmo nome de tabela escrito de três
jeitos diferentes em três notebooks. Toda nomeação passa por ele, e os demais o carregam com `%run ./_config`.

### 4.2 Os processos de ETL e o impacto de cada transformação

O pipeline é uma cadeia de ETLs, não um único:

```
API ONS ──► Volume ──► Bronze ──► Silver ──► Gold ──► Análise
            (arquivo)  (STRING)   (tipado)   (dimensional)
```

| # | Etapa | O que fiz | Por que fiz | Impacto nos dados |
|---|---|---|---|---|
| 1 | Volume → Bronze | Li os CSVs com `multiLine=true` e todas as colunas como `STRING` | Não perder informação e não partir registros com quebra de linha em campo texto | 2.902 registros lidos inteiros; 6.824.310 registros na Bronze, zero rejeitados |
| 2 | Bronze → Silver | `TRIM` em todo texto e sentinelas (`''`, `'-'`) para `NULL` | A capacidade vem com largura fixa; conjuntos de usinas trazem `ceg = '-'` | 6.123.072 registros de restrição com `ceg` passaram a ter `NULL` real |
| 3 | Bronze → Silver | `CAST` para `DOUBLE`, `TIMESTAMP`, `DATE` | Viabilizar agregação e ordenação temporal | Zero valores perdidos na conversão |
| 4 | Bronze → Silver | Criei a coluna `eh_agregado` para a linha `SIN` | Somar sem filtrar dobra a energia do país | Evita que a carga total dê 7.453,71 TWh em vez de 3.726,86 TWh |
| 5 | Bronze → Silver | `UNION` das restrições eólica e solar, com `cod_fonte` | Analisar as duas tecnologias juntas ou separadas | 6.560.832 registros em uma tabela única |
| 6 | Bronze → Silver | Calculei `corte_mwh = GREATEST(referência − geração, 0) × 0,5` quando há razão declarada | Medir a energia cortada | 61.438,6 GWh no período, validados contra a GNRa oficial |
| 7 | Bronze → Silver | `dropDuplicates` na chave de negócio | Tornar o pipeline idempotente | Zero duplicatas removidas (a fonte estava íntegra) |
| 8 | Silver → Gold | UNPIVOT das quatro colunas de geração | Transformar fonte em dimensão | 199.584 horas × 4 fontes = 798.336 linhas |
| 9 | Silver → Gold | Agreguei a restrição de 30 min para hora, somando energia e separando o corte por razão e origem | Alinhar o grão ao balanço e atribuir cada meia hora à sua razão | 6.560.832 → 3.280.416 linhas, sem perder energia |
| 10 | Silver → Gold | `JOIN` da usina com a capacidade pelo CEG | Enriquecer a dimensão com potência instalada | 15 de 248 usinas com potência (os 233 conjuntos não têm CEG) |
| 11 | Silver → Gold | Construí a `dim_data` com as datas de todos os fatos | Evitar chaves órfãs | Integridade referencial com zero órfãs (eram 4) |

### 4.3 O cálculo do corte e sua validação

```sql
corte_mwmed = GREATEST(val_geracaoreferencia − val_geracao, 0)   -- apenas quando há razão de restrição
corte_mwh   = corte_mwmed × 0,5                                  -- intervalo de 30 minutos
```

Embuti três decisões nessa fórmula:

- **Uso `val_geracaoreferencia`, e não `val_geracaoreferenciafinal`**, porque o dicionário do ONS esclarece que a
  referência final só é calculada nos intervalos com razão REL.
- **`GREATEST(..., 0)`** descarta diferenças negativas, que representam geração acima da referência — erro de
  previsão do recurso, não corte.
- **Só conto corte quando há razão declarada**, para não tratar desvios normais de previsão como corte.

Essa é a mesma regra que o ONS descreve para a **Geração Não Realizada Apurada (GNRa)**. Quando o ONS passou a
publicar a GNRa oficial, comparei os dois valores: meu cálculo soma **61.438.603 MWh** e a GNRa oficial soma
**61.439.589 MWh** — diferença de **0,0016%**, com correlação de **0,99999** registro a registro. A diferença de
986 MWh corresponde exatamente a 42 registros solares em que o ONS apurou GNRa sem declarar razão de restrição.

### 4.4 Correções que fiz depois de rodar o pipeline com todos os dados

A primeira versão do pipeline foi escrita a partir de uma amostra (um mês de restrição eólica). Rodando com a janela
completa, encontrei problemas que a amostra não mostrava, e os corrigi:

| Problema encontrado | Correção |
|---|---|
| `CAST` inválido levantava exceção com o modo ANSI ligado por padrão | Desliguei o modo ANSI em `_config` e passei a medir a perda de conversão no notebook 04 |
| Descrições com aspas simples quebravam o `COMMENT ON` com `PARSE_SYNTAX_ERROR` | Criei a função `esc_sql()` e passei a usá-la em todos os comentários |
| Registros com quebra de linha em campo texto eram partidos | `multiLine=true` na leitura da Bronze |
| O ONS republicou a restrição com 24 colunas | Incluí GNRa oficial, minutos por razão, ponto de conexão e agente na Silver |
| Os rótulos das razões de restrição não seguiam o dicionário, e o código PAR não era tratado | Rótulos oficiais para REL, CNF, ENE e PAR |
| A razão de cada hora era atribuída pelo primeiro valor encontrado | Colunas de corte por razão e por origem no fato horário |
| O JOIN da usina com a capacidade por nome casava 0 de 248 | JOIN pelo CEG |
| `dim_usina` podia ter mais de uma linha por usina se o nome mudasse | Atributos do registro mais recente por `id_ons` (`max_by`) |
| 4 chaves órfãs entre o fato de reservatório e a `dim_data` | `dim_data` cobrindo as datas de todos os fatos |

### 4.5 Evidências na plataforma

![Tabelas persistidas no schema bronze](docs/img/databricks/10-tabelas-bronze.png)

![Tabelas persistidas no schema silver](docs/img/databricks/11-tabelas-silver.png)

![Tabelas persistidas no schema gold](docs/img/databricks/12-tabelas-gold.png)

![Detalhe de uma tabela Delta no Catalog Explorer](docs/img/databricks/13-detalhe-tabela-delta.png)

---

## 5. Qualidade de Dados (Etapa 4.5)

**Notebook:** [`04_qualidade_dados.py`](04_qualidade_dados.py)

### 5.1 Como verifiquei

Rodei a perfilagem sobre a camada **Bronze**, onde o dado está como no momento da captura, como o enunciado pede.
As verificações que dependem de tipo (acurácia e outliers) usam a Silver. Usei como contrato de referência os
**dicionários de dados do ONS**, que dizem se cada campo pode ser nulo, zero ou negativo. Assim, cada achado é
confrontado com o que a fonte promete.

### 5.2 Completude

Todas as colunas do balanço e do EAR estão **100% completas**. As ausências se concentram na restrição e na
capacidade — e nenhuma delas é defeito da fonte.

| Tabela | Coluna | Ausente | O que significa | O que fiz |
|---|---|---|---|---|
| Restrição eólica / solar | `cod_razaorestricao`, `cod_origemrestricao`, `val_geracaolimitada`, `val_geracaonaorealizadaapurada`, `num_minutos_*` | 59,9% / 72,5% | **Não houve restrição** no intervalo | `NULL` real e coluna `houve_restricao` |
| Restrição eólica / solar | `val_geracaoreferenciafinal` | 96,6% / 98,0% | Só é calculada nos intervalos **REL** | Não uso como base do corte |
| Restrição eólica / solar | `dsc_restricao` | 74,1% / 81,2% | Campo criado em **26/09/2025**: vazio antes, 100% preenchido nos intervalos restritos depois | Mantido na Silver e usado como evidência na P5 |
| Restrição eólica / solar | `ceg` (sentinela `-`) | 92,9% / 94,3% | **Conjuntos de usinas** não têm CEG | `-` → `NULL`; JOIN com capacidade só casa usinas individuais |
| Capacidade | `dat_desativacao` | 82,7% | Unidade **ativa** | Coluna `esta_ativa` |
| Capacidade | `dat_entradateste` | 0,14% | O dicionário permite nulo | Nenhum |

A interpretação mais importante é a primeira. Um tratamento automático chamaria os 59,9% de `cod_razaorestricao`
vazio de "dado faltante" e sugeriria imputação. Mas o dicionário do ONS diz textualmente que *"se o campo for nulo,
não houve limitação"*. Preencher com um valor padrão teria inventado restrições que não existiram e inflado todo
o corte medido.

### 5.3 Unicidade

| Tabela | Chave de negócio | Linhas | Duplicadas |
|---|---|---|---|
| Balanço | `id_subsistema` + `din_instante` | 249.480 | 0 |
| EAR | `id_subsistema` + `ear_data` | 8.320 | 0 |
| Restrição eólica | `id_ons` + `din_instante` | 4.499.184 | 0 |
| Restrição solar | `id_ons` + `din_instante` | 2.061.648 | 0 |
| Capacidade | `cod_equipamento` + `nom_usina` | 5.678 | 0 |

Não encontrei duplicatas. Mesmo assim, mantive o `dropDuplicates` na Silver, porque o download é reexecutável e
uma reexecução parcial poderia reprocessar um arquivo já carregado. A deduplicação torna o pipeline idempotente.

### 5.4 Consistência

- **Perda no CAST:** testei timestamps, datas e valores numéricos das cinco tabelas. **Zero valores perdidos.**
- **Domínios categóricos:** `cod_razaorestricao` só assume REL, CNF e ENE (o código PAR, documentado, não ocorreu
  no período, mas já é tratado); `cod_origemrestricao` só assume SIS e LOC. O balanço tem cinco valores de
  subsistema (com SIN) e o EAR tem apenas os quatro reais.
- **Espaços de preenchimento:** 100% das linhas da capacidade trazem `nom_subsistema`, `cod_equipamento` e
  `num_unidadegeradora` completados com espaços à direita. Como o preenchimento é uniforme, o problema não aparece
  dentro da tabela, só na comparação com outras tabelas. Apliquei `TRIM` em todas as colunas de texto.
- **Registros multilinha:** 2.280 registros na eólica e 622 na solar têm quebra de linha dentro de campo texto.
  Resolvi com `multiLine=true`.
- **A linha SIN é um agregado:** a carga do SIN é **idêntica** à soma dos quatro subsistemas em todas as 49.896
  horas. Somar tudo sem filtrar dá exatamente o dobro.
- **Sinal do intercâmbio:** o dicionário não define o sinal. O intercâmbio tem correlação de 0,99 a 1,00 com
  `geração total − carga` em todos os subsistemas, então **positivo = exportação**.

### 5.5 Acurácia

| Teste | Violações | Avaliados | Leitura |
|---|---|---|---|
| Carga, geração hidráulica, térmica, eólica ou solar negativa (balanço) | 0 | 199.584 | Coerente com o dicionário |
| EAR fora de 0–100% | 0 | 8.320 | OK |
| Geração solar > 1 MWmed entre 0h e 4h | 6.300 | 199.584 | Maioria ruído de medição; picos isolados acima de 2 GW em 2024 são erro na fonte |
| Geração da usina acima da disponibilidade + 5% | 117.620 | 6.560.832 | Explicado pelo dicionário: geração inclui usinas em teste; disponibilidade, só as comerciais |
| Geração negativa em usina | 21 | 6.560.832 | Viola o dicionário; mínimo de −1,4 MWmed (consumo auxiliar). Neutralizado pelo `GREATEST` |
| Corte calculado negativo | 0 | 6.560.832 | Protegido pelo `GREATEST` |
| Restrição declarada sem energia cortada | 327.852 | 6.560.832 | 13,8% dos intervalos restritos: o limite estava acima do recurso disponível |
| Restrição sem geração de referência | 0 | 6.560.832 | OK |

**Solar noturna.** Não corrigi esses valores: eles representam **0,047% da energia solar** do período, e nenhuma
pergunta depende da geração solar na madrugada. Deixo registrado como limitação conhecida.

**Validação do corte contra a fonte oficial.** É a verificação de acurácia mais importante do MVP:

| Métrica | Valor |
|---|---|
| Corte calculado pelo pipeline | 61.438.603 MWh |
| GNRa oficial publicada pelo ONS | 61.439.589 MWh |
| Diferença | 986 MWh (0,0016%) |
| Correlação registro a registro | 0,99999 |
| Intervalos restritos que divergem mais de 0,01 MWmed | 28 de 2.374.166 |
| Registros com GNRa oficial sem razão declarada | 42 (todos solares; somam os 986 MWh da diferença) |

**Referência final.** `val_geracaoreferenciafinal` está preenchida em 100% dos 193.333 intervalos REL e em 0% dos
demais — o que confirma o dicionário e explica a ausência de 96–98%.

### 5.6 Outliers (IQR, critério de Tukey)

| Métrica | Mínimo | Q1 | Mediana | Q3 | Máximo | % outliers |
|---|---|---|---|---|---|---|
| Carga do subsistema (MWmed) | 1.364 | 8.937 | 12.546 | 23.618 | 62.150 | 7,5% |
| Geração eólica (MWmed) | 0 | 35 | 323 | 1.703 | 22.681 | 22,5% |
| Geração solar (MWmed) | 0 | 0,1 | 3,0 | 1.271 | 24.531 | 14,4% |
| Intercâmbio (MWmed) | −16.172 | −4.293 | −412 | 4.247 | 15.300 | 0,0% |
| EAR (%) | 16,5 | 54,0 | 67,6 | 85,0 | 99,8 | 0,0% |
| Corte por usina / 30 min (MWh) | 0 | 2,5 | 11,7 | 31,0 | 618,1 | 8,9% |

**Não removi nenhum outlier, e é uma decisão deliberada minha.** Carga e geração misturam, na mesma coluna,
subsistemas de portes muito diferentes: os "outliers" são simplesmente o Sudeste. A geração solar é bimodal —
zero à noite, alta ao meio-dia —, e o IQR classifica os picos legítimos como extremos. O corte é um fenômeno de
cauda, e os eventos extremos são justamente o que quero investigar. Uso a perfilagem para escolher a estatística
certa (mediana, agregação por subsistema e por período), não para descartar a realidade medida.

### 5.7 Cobertura temporal

O balanço tem exatamente **24 registros por dia por subsistema em todos os anos**, sem lacunas (2026 tem 253 dias
porque o ONS publica até a data mais recente). Na restrição, cada usina tem entre ~1.340 e ~1.490 registros por
mês (48 por dia), e o número de usinas monitoradas cresce de 221 para 236 ao longo do período. No fato horário,
todas as 3.280.416 horas têm os dois intervalos de 30 minutos esperados.

### 5.8 Síntese: problemas detectados e como resolvi

| # | Problema | Dimensão | Como resolvi |
|---|---|---|---|
| 1 | Linha agregada `SIN` misturada aos subsistemas no balanço | Acurácia | `eh_agregado` na Silver; fatos Gold filtram |
| 2 | Ausências com significado (sem restrição, só REL, campo novo, conjunto sem CEG, unidade ativa) | Completude | `NULL` real, colunas booleanas derivadas, sem imputação |
| 3 | Espaços de preenchimento em 100% da capacidade | Consistência | `TRIM` em todo texto |
| 4 | Registros com quebra de linha em campo texto | Consistência | `multiLine=true` |
| 5 | Sentinela `-` no CEG | Consistência | `-` → `NULL` |
| 6 | Tudo como texto na Bronze | Consistência | `CAST` com perda medida (zero) |
| 7 | JOIN por nome com a capacidade casando 0 de 248 | Consistência | JOIN pelo CEG |
| 8 | Chaves órfãs entre EAR e calendário | Integridade | `dim_data` com as datas de todos os fatos |
| 9 | Solar noturna com picos de erro em 2024 | Acurácia | Documentado; 0,047% da energia solar, sem impacto nas respostas |
| 10 | Geração negativa em 21 registros de usina | Acurácia | Mantido; neutralizado pelo `GREATEST` |
| 11 | Risco de duplicatas em recarga | Unicidade | `dropDuplicates` (zero encontradas) |

A conclusão da etapa é que os dados do ONS são **tecnicamente limpos**. Os riscos reais estavam na **semântica**,
não na sujeira. Nenhum desses problemas geraria erro de execução; todos gerariam números errados e plausíveis.
Foi a leitura do dicionário da fonte, e não a perfilagem sozinha, que me permitiu tratá-los corretamente.

### 5.9 Evidências na plataforma

![Perfil de completude no notebook 04](docs/img/databricks/14-qualidade-completude.png)

![Unicidade e perda no CAST](docs/img/databricks/15-qualidade-unicidade-cast.png)

![Testes de acurácia e validação contra a GNRa oficial](docs/img/databricks/16-qualidade-acuracia-gnra.png)

![Análise de outliers](docs/img/databricks/17-qualidade-outliers.png)

---

## 6. Análise de Dados (Etapa 4.5)

**Notebook:** [`05_analise_perguntas.py`](05_analise_perguntas.py)

### 6.1 Método

Cada pergunta tem uma seção própria no notebook, com a consulta SQL sobre o modelo dimensional, a tabela de
resultado, um gráfico quando ajuda a leitura, e a discussão. **Nas discussões do notebook não há números digitados
à mão:** cada valor é calculado na execução e interpolado no texto. E quando a conclusão depende do resultado —
por exemplo, qual razão de corte predomina — o notebook escolhe a interpretação a partir do número, em vez de
afirmar uma conclusão decidida antes de olhar os dados.

Os números abaixo são os da minha execução, com dados coletados em 13/09/2026. Quando comparo anos, uso **2025,
o último ano completo**, porque 2026 está incompleto e concentrado no período úmido.

---

### P1 — Participação das fontes na geração

> *Como evoluiu a participação de cada fonte (hidráulica, térmica, eólica e solar) na geração, por subsistema, ao
> longo do período?*

![P1 — Composição da geração por fonte, por subsistema](docs/img/p1_composicao_geracao.png)

| Fonte | Brasil 2021 | Brasil 2025 | Nordeste 2021 | Nordeste 2025 |
|---|---|---|---|---|
| Hidráulica | 62,7% | 57,7% | 22,6% | 16,5% |
| Térmica | 23,9% | 12,8% | 21,5% | 4,0% |
| Eólica | 12,2% | 16,5% | 51,8% | 62,4% |
| Solar | 1,3% | 13,1% | 4,2% | 17,1% |
| **Eólica + solar** | **13,4%** | **29,6%** | **56,0%** | **79,5%** |

Participação da solar por subsistema, 2021 → 2025: Norte 0,0% → 5,6% · Nordeste 4,2% → 17,1% ·
Sudeste/C.O. 0,7% → 13,6% · Sul 0,0% → 11,5%.

**Minha resposta.** No Brasil, a geração eólica + solar **mais que dobrou** a sua participação entre 2021 e 2025, de
13,4% para 29,6% da geração verificada. A solar foi a fonte que mais cresceu — de 1,3% para 13,1% —, e cresceu em
todos os subsistemas, com os maiores saltos no Sudeste/C.O. e no Nordeste. A térmica caiu quase à metade; em 2021,
ano da crise hídrica, ela ainda respondia por 23,9% da geração nacional.

O Nordeste é o caso extremo: eólica + solar passaram de 56,0% para **79,5%** da geração do subsistema, e a térmica
praticamente saiu da matriz (de 21,5% para 4,0%). Isso importa para o problema porque eólica e solar são as duas
fontes **não despacháveis**. Quando elas respondem por quatro quintos da geração de um subsistema, o operador perde
a alavanca que usaria para equilibrar oferta e demanda — e, quando há excesso, sobra cortar.

---

### P2 — Horas e meses de maior participação intermitente

> *Em quais horas do dia e meses do ano a geração intermitente (eólica + solar) atinge maior participação sobre a
> carga do subsistema?*

![P2 — Nordeste 2025: geração intermitente como % da carga](docs/img/p2_heatmap_intermitente_ne.png)

| Métrica (Nordeste, 2025) | Valor |
|---|---|
| Geração intermitente sobre a carga, no ano | 117,1% |
| Horas do ano em que eólica + solar superaram a carga | 72,0% |
| Combinações mês × hora acima de 100% | 213 de 288 |
| Maior participação | 173,7% — setembro, às 6h |
| Menor participação | 56,3% — janeiro, às 18h |
| Hora de maior participação média | 7h (141,9%) |
| Hora de menor participação média | 17h (91,2%) |
| Meses de maior participação | Setembro (148,3%), julho (141,0%), agosto (140,6%), outubro (136,4%) |
| Meses de menor participação | Janeiro (76,1%), abril (95,6%), fevereiro (99,1%) |

**Minha resposta.** Em 2025, o Nordeste gerou com eólica + solar **117,1% da própria carga** — e em 72,0% das horas
do ano essas duas fontes sozinhas superaram todo o consumo do subsistema.

O mapa de calor mostra duas estruturas sobrepostas. A **sazonal** vem do vento: os meses de maior participação são
os de julho a outubro, a safra de ventos do Nordeste, quando a faixa escura atravessa a madrugada inteira. A
**diária** tem o ponto mais alto no início da manhã (6h a 8h), quando o vento noturno ainda está forte e a solar
começa a entrar, e o vale no fim da tarde (17h e 18h), quando a solar desaparece, o vento ainda não voltou à
intensidade noturna e a carga sobe. O ponto crítico do sistema é a combinação de mês de vento forte com as
primeiras horas da manhã.

---

### P3 — Volume de energia cortada

> *Qual o volume total de energia eólica e solar cortada por constrained-off, e como evoluiu ao longo do tempo?*

![P3 — Energia cortada por mês](docs/img/p3_corte_mensal.png)

| Métrica (jan/2025 a ago/2026) | Valor |
|---|---|
| Energia cortada (meu cálculo) | **61.438,6 GWh** |
| GNRa oficial do ONS | 61.439,6 GWh |
| Energia efetivamente gerada | 233.970,3 GWh |
| Geração de referência (o que poderia ter sido gerado) | 292.597,1 GWh |
| **Taxa de corte** | **21,0%** |
| Eólica | 42.424,4 GWh (69,1% do corte) — taxa de 19,3% |
| Solar | 19.014,2 GWh (30,9% do corte) — taxa de 26,2% |
| 2025 completo | 37.216,3 GWh — taxa de 21,2% |
| Janeiro a agosto: 2025 → 2026 | 20.140,0 → 24.222,3 GWh (**+20,3%**); taxa de 18,7% → 20,7% |
| Mês de maior corte | outubro/2025 — 6.184,6 GWh |
| Mês de menor corte | fevereiro/2026 — 849,6 GWh |

**Minha resposta.** Em 20 meses, **61.438,6 GWh** de energia eólica e solar deixaram de ser gerados por ordem do
operador — **21,0%** de tudo o que essas usinas poderiam ter produzido. O número reproduz a GNRa oficial do ONS com
diferença de 0,0016%, então não é uma estimativa minha: é a apuração oficial reproduzida pelo pipeline.

Para dar escala, é o equivalente ao consumo residencial de cerca de **19,2 milhões de domicílios** brasileiros
médios ao longo do período (~160 kWh/mês, referência da EPE). A eólica corta mais energia em volume, mas a solar
perde uma parcela maior do que poderia gerar (26,2% contra 19,3%). A série tem sazonalidade clara, com pico no
segundo semestre (safra de ventos). E o problema está crescendo: nos mesmos oito meses do ano, o corte foi 20,3%
maior em 2026 do que em 2025.

---

### P4 — Onde os cortes se concentram

> *Onde os cortes se concentram — quais subsistemas, estados e usinas?*

![P4 — Energia cortada por estado](docs/img/p4_corte_por_estado.png)

| Subsistema | Energia cortada | % do total | Taxa de corte | Usinas |
|---|---|---|---|---|
| Nordeste | 51.294,2 GWh | 83,5% | 21,0% | 205 |
| Sudeste/C.O. | 9.007,3 GWh | 14,7% | **25,5%** | 29 |
| Sul | 803,9 GWh | 1,3% | 7,9% | 13 |
| Norte | 333,2 GWh | 0,5% | 10,2% | 1 |

| Estado | Energia cortada | % do total | Taxa de corte | Usinas |
|---|---|---|---|---|
| Rio Grande do Norte | 19.109,1 GWh | 31,1% | 27,0% | 67 |
| Bahia | 17.898,3 GWh | 29,1% | 20,0% | 65 |
| Minas Gerais | 8.138,4 GWh | 13,2% | 26,0% | 20 |
| Piauí | 6.088,9 GWh | 9,9% | 14,8% | 18 |
| Ceará | 4.537,9 GWh | 7,4% | 25,3% | 32 |
| Pernambuco | 1.868,2 GWh | 3,0% | 19,0% | 13 |

Maiores volumes por usina: Conj. Janaúba (MG, solar) 1.686,6 GWh · Conj. Arinos 2 500 kV (MG, solar) 1.120,1 GWh ·
Conj. Caju (RN, eólica) 1.057,2 GWh · Conj. Rio do Vento (RN, eólica) 1.024,0 GWh · Conj. Vista Alegre - Janaúba
(MG, solar) 982,3 GWh. As dez maiores somam 16,8% do total.

**Minha resposta.** Em **volume**, o corte é um problema do **Nordeste**: 83,5% de toda a energia cortada no país
está lá. Rio Grande do Norte e Bahia, sozinhos, somam 60,2%, e os três maiores estados, 73,5%.

Mas volume e intensidade contam histórias diferentes, e por isso separei as duas métricas. O **Sudeste/Centro-Oeste
tem a maior taxa de corte (25,5%)**, mesmo com volume bem menor. A explicação está em Minas Gerais, terceiro estado
em volume, onde grandes complexos solares no norte do estado — Janaúba e Arinos, que lideram o ranking por usina —
perdem 26% da produção possível. Existe, portanto, um segundo foco de corte fora do Nordeste, ligado à expansão
solar.

No nível da usina, o corte é espalhado: as dez maiores somam só 16,8% do total, e todas as 248 usinas monitoradas
tiveram algum corte. Não é um problema de meia dúzia de empreendimentos mal conectados — é um problema de sistema,
com endereço regional.

---

### P5 — Razão predominante dos cortes

> *Qual a razão predominante dos cortes: restrição elétrica/confiabilidade (escoamento) ou razão energética
> (excesso de oferta)? Isso mudou ao longo do tempo?*

Respondo com os códigos oficiais do dicionário do ONS: **REL** (indisponibilidade externa elétrica) e **CNF**
(requisitos de confiabilidade) são razões de **rede**; **ENE** (razão energética) significa que o sistema não tinha
como absorver a energia; **PAR** (parecer de acesso) não ocorreu no período.

![P5 — Composição mensal da energia cortada por razão](docs/img/p5_razao_mensal.png)

| Razão | Energia cortada | % |
|---|---|---|
| **Razão energética (ENE)** | **36.599,6 GWh** | **59,6%** |
| Confiabilidade (CNF) | 17.110,0 GWh | 27,8% |
| Indisponibilidade externa (REL) | 7.729,0 GWh | 12,6% |
| Parecer de acesso (PAR) | 0,0 GWh | 0,0% |
| *Razões de rede (REL + CNF)* | *24.839,0 GWh* | *40,4%* |

| Recorte | ENE | CNF | REL |
|---|---|---|---|
| Eólica | 54,5% | 32,3% | 13,2% |
| Solar | 70,9% | 18,0% | 11,2% |
| jan/2025 a jul/2025 | 48,4% | 25,5% | 26,1% |
| ago/2025 a fev/2026 | 55,0% | 38,6% | 6,4% |
| mar/2026 a ago/2026 | **74,0%** | 16,0% | 10,0% |

Origem das restrições: **84,0% sistêmica** e 16,0% local.

Principais descrições do ONS (campo disponível a partir de set/2025), por energia cortada:

| Razão | Descrição publicada pelo ONS | Energia |
|---|---|---|
| ENE | *Controle de frequência do SIN* | 26.478,3 GWh |
| CNF | *Controle de carregamento da transformação 500/345 kV da SE Rio Novo do Sul* | 3.702,4 GWh |
| CNF | *Limitação da transmissão na LT 500 kV Açu III / Jaguaruana II – C1* | 3.335,4 GWh |
| CNF | *Limitação da transmissão nas LTs 500 kV Açu III / Quixadá, Açu III / Milagres II e Açu III / Jaguaruana II* | 1.483,9 GWh |
| REL | *Controle do fluxo Nordeste/Sudeste (FNESE), conforme solicitação de intervenção* | 910,5 GWh |

**Minha resposta.** O motivo predominante é **energético**: **59,6%** da energia cortada, contra 40,4% somando as duas
razões de rede. Ou seja, na maior parte do tempo a energia não foi cortada porque faltava linha para levá-la, e sim
porque **o sistema como um todo não tinha como absorvê-la naquele instante**. As descrições do ONS confirmam essa
leitura: a razão energética aparece como *"controle de frequência do SIN"* — o operador corta renováveis para manter
a frequência quando a geração supera o que a carga e os intercâmbios conseguem absorver. A origem é 84,0%
sistêmica, o que reforça que o corte é decidido olhando o sistema, e não a usina isolada.

As razões de rede, porém, não são pequenas (40,4%), e as descrições apontam gargalos físicos com endereço: as linhas
de 500 kV que partem da subestação Açu III, no Rio Grande do Norte, e a transformação da SE Rio Novo do Sul, no
Espírito Santo.

**E sim, a composição mudou.** O início de 2025 teve peso alto de indisponibilidade externa — em fevereiro/2025, a
REL respondeu por 77,6% do corte, concentrada no Nordeste. A partir daí, o corte ficou cada vez mais energético: a
participação da ENE subiu de 48,4% para **74,0%** entre a primeira e a última janela, chegando a 84,3% em
março/2026. A solar, que corta sobretudo nas horas de sol, quando há excesso de oferta, é ainda mais energética
(70,9%) que a eólica (54,5%).

A distinção importa porque cada razão pede uma solução diferente. Corte por REL ou CNF se resolve com transmissão.
Corte por razão energética **não se resolve com linha nova**: exige flexibilidade — armazenamento, deslocamento de
consumo, redução da geração inflexível ou mais capacidade de exportação.

---

### P6 — Reservatórios × cortes

> *Existe relação entre o nível dos reservatórios (EAR) e a intensidade dos cortes?*

Minha hipótese era: reservatório cheio significa menos espaço para "guardar" energia reduzindo a geração hidráulica;
com a hidráulica já no mínimo e renovável em excesso, sobraria cortar. Testei com o EAR do próprio Nordeste e com o
do **Sudeste/Centro-Oeste**, que concentra a maior capacidade de armazenamento do país e está acoplado ao Nordeste
pelo intercâmbio.

![P6 — Nível dos reservatórios × energia cortada por dia no Nordeste](docs/img/p6_reservatorio_corte.png)

| Correlação com o corte diário do Nordeste (608 dias) | Pearson | Spearman | Pearson sem sazonalidade |
|---|---|---|---|
| EAR do Nordeste (44,0% a 96,5%) | −0,13 | −0,10 | +0,14 |
| EAR do Sudeste/C.O. (40,0% a 70,5%) | −0,17 | −0,18 | −0,02 |

| Faixa de armazenamento do Nordeste | Dias | Corte médio por dia | Taxa média |
|---|---|---|---|
| Baixo (30–50%) | 90 | 91,1 GWh | 20,7% |
| Confortável (50–70%) | 188 | 95,5 GWh | 19,5% |
| Alto (>70%) | 330 | 76,2 GWh | 17,4% |

**Minha resposta.** **Não, a hipótese não se sustenta.** As correlações são fracas com o EAR do Nordeste (−0,13) e com
o do Sudeste (−0,17) — e têm sinal **negativo**, o oposto do que eu previa. Nas faixas de armazenamento, os dias
com reservatório mais cheio no Nordeste tiveram, em média, **menos** corte, e não mais.

O sinal negativo, porém, não é uma relação direta: é sazonalidade. Os reservatórios enchem no período úmido
(verão), quando o vento é mais fraco, e o corte é máximo na safra de ventos (julho a outubro), quando os
reservatórios já estão deplecionando. Para separar essa causa comum, removi a média de cada mês das duas séries: a
correlação passa a **+0,14 no Nordeste (fraca) e −0,02 no Sudeste (inexistente)**.

Concluo que o nível dos reservatórios não é um fator explicativo relevante para o corte no período. Isso é coerente
com a P5: o corte energético é decidido pelo controle de frequência a cada momento e depende do balanço instantâneo
entre geração, carga e exportação, muito mais do que do estoque de água armazenada.

---

### P7 — Carga líquida e intercâmbio no Nordeste

> *Em que medida a carga líquida (carga − eólica − solar) do Nordeste fica negativa, e como o intercâmbio responde a
> isso?*

![P7 — Nordeste: perfil horário médio](docs/img/p7_perfil_horario_ne.png)

| Métrica (Nordeste, jan/2025 a set/2026 — 14.832 horas) | Valor |
|---|---|
| Horas com carga líquida **negativa** | 10.275 (**69,3%**) |
| Menor carga líquida registrada | −12.896 MWmed |
| Hora com carga líquida média mais negativa | 8h (−5.182 MWmed; negativa em 89,3% dos dias) |
| Horas do dia com carga líquida média positiva | 16h a 20h |
| Horas em que o Nordeste exporta | 92,0% |
| Maior exportação registrada | 15.300 MWmed |
| Correlação carga líquida × intercâmbio (hora a hora) | **−0,91** |
| Exportação média com carga líquida negativa / positiva | 8.166 / 1.522 MWmed |
| Corte médio por hora com carga líquida negativa / positiva | 4.324 / 1.711 MWh (2,5 vezes) |
| Parcela do corte do Nordeste em horas de carga líquida negativa | **84,9%** |

| Ano | Horas com carga líquida negativa no Nordeste |
|---|---|
| 2021 | 11,7% |
| 2022 | 32,8% |
| 2023 | 51,0% |
| 2024 | 62,4% |
| 2025 | 72,0% |
| 2026 (até set) | 65,3% |

**Minha resposta.** Carga líquida negativa significa que, naquela hora, eólica e solar sozinhas geraram mais do que
todo o consumo do Nordeste. Isso deixou de ser exceção: desde 2025 acontece em **69,3% das horas**. A série anual
mostra a velocidade da mudança — de 11,7% das horas em 2021 para 72,0% em 2025. No perfil médio, a carga líquida só
fica positiva entre 16h e 20h, quando a solar desaparece.

Quando há excedente, o sistema tem três saídas: exportar, armazenar ou cortar. O Brasil praticamente não tem
armazenamento em escala de rede, e o intercâmbio mostra a exportação trabalhando no limite: a correlação de −0,91
indica que, quanto maior o excedente, mais o Nordeste exporta — em média 8.166 MWmed nas horas de carga líquida
negativa, contra 1.522 MWmed nas demais. O Nordeste exporta em 92% das horas.

Mas a exportação tem teto, e o corte aparece quando esse teto é atingido: **84,9% de toda a energia cortada no
Nordeste ocorre em horas de carga líquida negativa**, e o corte médio nessas horas é 2,5 vezes maior. É o mecanismo
físico por trás da P5: quando a exportação satura por limite de segurança, a razão declarada é de confiabilidade;
quando o sistema inteiro não consegue absorver o excedente, é energética.

---

### P8 — Usinas que mais perdem

> *Quais usinas têm a maior taxa de corte relativa à sua própria geração de referência?*

![P8 — Usinas com maior taxa de corte](docs/img/p8_usinas_taxa_corte.png)

| # | Usina | UF | Fonte | Taxa de corte | Energia cortada | Geração de referência | Potência |
|---|---|---|---|---|---|---|---|
| 1 | Rei dos Ventos 3 | RN | Eólica | **48,4%** | 117,4 GWh | 242,7 GWh | 60,1 MW |
| 2 | Conj. BJL | BA | Solar | 46,1% | 76,9 GWh | 166,8 GWh | conjunto |
| 3 | Rei dos Ventos 1 | RN | Eólica | 45,5% | 104,4 GWh | 229,2 GWh | 58,5 MW |
| 4 | Conj. Tacaratu | PE | Solar | 45,5% | 11,8 GWh | 26,0 GWh | conjunto |
| 5 | Conj. Cumaru | RN | Eólica | 44,4% | 686,3 GWh | 1.544,8 GWh | conjunto |
| 6 | Alegria II | RN | Eólica | 44,2% | 131,9 GWh | 298,8 GWh | 100,7 MW |
| 7 | Conj. Santa Clara | RN | Eólica | 43,4% | 371,0 GWh | 855,9 GWh | conjunto |
| 8 | Conj. São Pedro | BA | Solar | 42,8% | 90,0 GWh | 210,2 GWh | conjunto |
| 9 | Conj. Sol do Sertão | BA | Solar | 42,1% | 710,0 GWh | 1.685,0 GWh | conjunto |
| 10 | Conj. Horizonte | BA | Solar | 42,1% | 147,2 GWh | 349,4 GWh | conjunto |
| 11 | Miassaba 3 | RN | Eólica | 42,0% | 121,8 GWh | 290,2 GWh | 68,5 MW |
| 12 | Conj. Ituverava | BA | Solar | 41,9% | 347,9 GWh | 830,4 GWh | conjunto |

| Distribuição da taxa de corte (243 usinas com mais de 10 GWh de referência) | Valor |
|---|---|
| Mediana | **23,9%** |
| Percentil 90 | 35,4% |
| Máxima | 48,4% |
| Usinas acima de 20% | 155 (63,8%) |
| Usinas abaixo de 5% | 2 |
| Média ponderada do sistema | 21,0% |
| Top 20 por estado | RN 10 · BA 8 · PE 1 · MG 1 |
| Top 20 por fonte | Eólica 10 · Solar 10 |

**Minha resposta.** A usina mais afetada é a **Rei dos Ventos 3** (RN, eólica), que perdeu **48,4%** da geração
possível — 2,3 vezes a média do sistema. As maiores taxas se concentram no **Rio Grande do Norte** (10 das 20
maiores) e na **Bahia** (8), igualmente divididas entre eólicas e solares.

O corte não é distribuído de forma equânime, mas também não é um problema de poucas usinas. A usina típica perdeu
**23,9%** da produção possível, 63,8% das usinas perderam mais de 20%, e só duas perderam menos de 5%. As maiores
taxas estão nos estados cujos gargalos aparecem nas descrições de restrição por confiabilidade (P5), como as linhas
de 500 kV que partem de Açu III, no Rio Grande do Norte.

A consequência econômica é direta: duas usinas com o mesmo custo de capital e o mesmo recurso natural podem ter
receitas muito diferentes dependendo apenas de onde foram conectadas. É um risco locacional que um estudo de
viabilidade baseado só no recurso do terreno não captura.

Duas notas de método: o piso de 10 GWh de referência impede que uma usina com poucas semanas de histórico apareça no
topo por ruído estatístico; e a potência instalada só está disponível para as usinas individuais, porque os
conjuntos não têm CEG e não casam com o cadastro de capacidade.

---

### 6.2 Discussão geral: conectando as respostas ao problema

O problema que formulei foi: a expansão de eólica e solar, concentrada no Nordeste, cresceu mais rápido do que a
capacidade do sistema de absorvê-la, e o operador passou a cortar geração renovável disponível. Quanto se perde,
onde, quando e por quê?

As oito respostas formam um argumento único:

1. **Causa estrutural (P1).** Eólica + solar foram de 56,0% para 79,5% da geração do Nordeste, e de 13,4% para
   29,6% no país. Cresceu justamente o que o operador não despacha.
2. **Quando (P2).** Em 2025, essas fontes superaram a carga do Nordeste em 72,0% das horas, com pico nas manhãs da
   safra de ventos.
3. **Quanto (P3).** 61.438,6 GWh cortados em 20 meses, 21,0% da geração possível, valor validado contra a apuração
   oficial — e crescendo 20,3% de um ano para o outro.
4. **Onde (P4).** 83,5% do volume está no Nordeste; a maior taxa, no Sudeste/C.O., pela solar de Minas Gerais.
5. **Por quê (P5).** 59,6% por razão energética e 40,4% por razões de rede, com a razão energética ganhando peso.
6. **O que não explica (P6).** O nível dos reservatórios não tem relação relevante com o corte.
7. **Mecanismo (P7).** A carga líquida do Nordeste é negativa em 69,3% das horas; a exportação absorve o excedente
   até o limite, e 84,9% do corte acontece quando esse limite é atingido.
8. **Quem perde (P8).** A usina típica perde 23,9% da produção possível; as piores, perto de metade.

**Conclusão.** No período analisado, o corte de renováveis no Brasil é **predominantemente energético**: o sistema não
consegue absorver, a cada momento, toda a geração renovável disponível. Na maior parte do tempo, o corte não é causado
por falta de linhas de transmissão.

A implicação para decisão é que **reforço de transmissão, sozinho, não resolve o problema**. Ele atacaria a parcela
de rede (40,4%), concentrada em gargalos que o próprio ONS identifica, como os troncos de 500 kV de Açu III. A parcela
energética (59,6%) pede flexibilidade: armazenamento, deslocamento de consumo para as horas de excedente, redução da
geração inflexível e mais capacidade de exportação entre subsistemas. E a entrada de novas usinas intermitentes, sem
essa flexibilidade, tende a aumentar a taxa de corte de todas as que já existem.

### 6.3 Ressalvas metodológicas

1. **O corte depende da geração de referência estimada pelo ONS.** Meu cálculo reproduz a apuração oficial, mas
   herda as premissas de estimativa do recurso eólico e solar usadas pelo operador.
2. **Correlação não é causalidade (P6).** Tratei a sazonalidade removendo a média mensal, mas outros fatores comuns
   não foram controlados.
3. **A janela de constrained-off é de 20 meses.** Suficiente para medir e comparar anos, mas não para uma tendência de
   longo prazo.
4. **P2 e P7 analisam só o Nordeste**, o subsistema com a maior participação intermitente.
5. **Solar noturna com erros pontuais em 2024** (0,047% da energia solar) não foi corrigida.
6. **A potência instalada existe só para 15 das 248 usinas** (P8), porque os conjuntos não têm CEG.

### 6.4 Evidências na plataforma

![P1 no notebook 05](docs/img/databricks/18-p1.png)

![P2 no notebook 05](docs/img/databricks/19-p2.png)

![P3 no notebook 05](docs/img/databricks/20-p3.png)

![P4 no notebook 05](docs/img/databricks/21-p4.png)

![P5 no notebook 05](docs/img/databricks/22-p5.png)

![P6 no notebook 05](docs/img/databricks/23-p6.png)

![P7 no notebook 05](docs/img/databricks/24-p7.png)

![P8 no notebook 05](docs/img/databricks/25-p8.png)

![Síntese no notebook 05](docs/img/databricks/26-sintese.png)

---

## 7. Autoavaliação

### 7.1 Consegui atingir os objetivos?

O objetivo que declarei na etapa 2 era construir um pipeline de ponta a ponta capaz de dimensionar e explicar o corte
de geração renovável no SIN. **Considero o objetivo atingido.**

**Do lado da engenharia**, implementei o ciclo completo: coleta automatizada via API, aterrissagem em Volume com
preservação do arquivo original, três camadas medalhão, modelo dimensional com dimensões conformes, catálogo de dados
gerado pelo próprio pipeline, integridade referencial verificada, qualidade persistida e validação do resultado
contra a apuração oficial da fonte. O pipeline é idempotente e parametrizável na janela temporal.

**Do lado analítico**, respondi às oito perguntas:

| Pergunta | Situação | Observação |
|---|---|---|
| P1 | Respondida | Seis anos de dados, quatro subsistemas |
| P2 | Respondida | Analisei o Nordeste, onde a pergunta é mais relevante |
| P3 | Respondida | Validada contra a GNRa oficial; a janela de 20 meses limita a leitura de tendência |
| P4 | Respondida | Subsistema, estado e usina |
| P5 | Respondida | Com as categorias oficiais do ONS e evidência textual das descrições |
| P6 | Respondida | A resposta é negativa: a hipótese não se sustentou |
| P7 | Respondida | Incluí a relação horária entre carga líquida e corte |
| P8 | Respondida | Potência instalada disponível só para 15 usinas individuais |

### 7.2 O que os dados me limitaram

- **P8 (potência instalada).** Eu queria cruzar a taxa de corte com o porte das usinas, mas 233 das 248 entradas da
  restrição são conjuntos de usinas, que não têm CEG e não existem no cadastro de capacidade. A resposta sobre quais
  usinas mais perdem está completa; a análise por porte ficou restrita a 15 usinas.
- **P3 (tendência).** Com 20 meses de restrição, consigo afirmar que o corte cresceu entre 2025 e 2026, mas não
  consigo descrever uma tendência de vários anos.
- **P6.** A pergunta foi respondida, mas com uma resposta negativa. Considero isso um resultado, não uma falha: a
  hipótese dos reservatórios era plausível e os dados a descartaram.

### 7.3 Hipóteses que os dados derrubaram

Esta foi a parte mais instrutiva do trabalho. **Antes de rodar o pipeline com a janela completa, eu tinha expectativas
que os números contrariaram:**

- **Eu esperava que o corte fosse predominantemente um problema de rede.** Minha primeira redação da conclusão dizia
  isso. Os dados mostraram o contrário: 59,6% do corte é energético.
- **Eu esperava que reservatórios cheios aumentassem o corte.** A correlação saiu fraca e negativa, e sem sustentação
  depois de remover a sazonalidade.
- **Eu esperava que o `TRIM` resolvesse o JOIN com o cadastro de capacidade.** Casou 0 de 248, por diferença de caixa
  e de granularidade.
- **Eu tinha concluído, a partir de uma amostra de um mês, que `dsc_restricao` era sempre vazio.** Na verdade o campo
  só passou a existir em setembro de 2025.

Foi por isso que reescrevi o notebook de análise para que a conclusão principal seja escolhida a partir do resultado,
e não afirmada antes dele.

### 7.4 Dificuldades que encontrei

- **Restrição de rede da Databricks Free Edition.** A coleta por API depende de acesso a domínios que a Free Edition
  bloqueia por padrão. Precisei documentar a verificação da conta pelo LinkedIn e um plano B de upload manual.
- **Modo ANSI do Databricks.** Com o modo ANSI ligado por padrão, um `CAST` inválido interrompe a carga em vez de
  gerar `NULL`. Desliguei o modo ANSI e passei a medir a perda de conversão explicitamente.
- **Aspas simples nos comentários do catálogo.** Descrições com aspas quebravam o `COMMENT ON` com
  `PARSE_SYNTAX_ERROR`, o que me levou a criar a função `esc_sql()`.
- **Registros com quebra de linha em campo texto**, que o Spark partia até eu habilitar `multiLine`.
- **A fonte mudou durante o projeto.** O ONS republicou os arquivos de restrição com 24 colunas e um novo dicionário,
  e o volume dobrou em relação ao que eu tinha estimado.
- **Descobrir as URLs reais dos arquivos**, que não seguem o nome do pacote CKAN.
- **Granularidades incompatíveis** entre as fontes, que me levaram à constelação de fatos.
- **Interpretar a semântica dos dados.** A linha SIN, as ausências com significado, a referência final só para REL, o
  sinal do intercâmbio não documentado: foram as dificuldades com maior potencial de produzir números errados sem
  nenhum erro de execução.

### 7.5 Trabalhos futuros

**Curto prazo — ampliar o que já existe:**
- Ampliar a janela de constrained-off: o ONS publica a restrição eólica mensal há cerca de cinco anos e a solar há
  cerca de dois anos e meio, o que permitiria analisar tendência de longo prazo.
- Incorporar o dataset de geração por usina para ligar os conjuntos às usinas individuais e recuperar a potência
  instalada e o fator de capacidade.

**Médio prazo — engenharia:**
- Converter a carga full-refresh em **carga incremental**, processando só os arquivos novos.
- Orquestrar os notebooks com **Databricks Workflows**, com dependências e agendamento.
- Transformar as verificações de qualidade em **expectativas declarativas** (Lakeflow Declarative Pipelines), que
  barrem a carga quando uma regra crítica for violada — em especial a divergência entre o corte e a GNRa oficial.
- Otimizar `fato_restricao_horaria` com clustering por `sk_data` e `sk_usina`.

**Longo prazo — análise:**
- Estimar o **valor econômico** da energia cortada, cruzando com o PLD horário da CCEE.
- Cruzar com os **limites de intercâmbio entre subsistemas** (como o FNESE) para medir diretamente a saturação da
  exportação que a P7 sugere.
- Construir um **modelo preditivo de risco de corte por usina**, a partir de previsão de vento, irradiação, carga e
  disponibilidade de rede.
- Separar o efeito da **geração distribuída** sobre a carga líquida do Nordeste e do Sudeste.

### 7.6 O que este trabalho consolidou para mim

Cinco coisas ficaram claras para mim ao construir este pipeline, e nenhuma delas era óbvia quando comecei.

**A camada Bronze só serve para alguma coisa se eu resistir à tentação de "já arrumar" o dado ali.** Ler tudo como
`STRING` parece desperdício até o momento em que preciso medir a qualidade da fonte. Se eu tivesse convertido tipos
na leitura, os registros problemáticos teriam virado `NULL` silenciosamente e, na etapa 4.5, eu estaria medindo a
minha própria limpeza em vez do dado que o ONS publicou.

**Meu problema mais caro não foi técnico, foi de interpretação.** A linha `SIN` não quebra nada: ela devolve um número
plausível, que por acaso é o dobro do correto. O mesmo vale para os 59,9% de `cod_razaorestricao` vazio na eólica,
que um tratamento automático classificaria como dado faltante — quando a ausência, ali, significa "não houve
restrição". Aprendi que perfilagem sem leitura de domínio produz diagnósticos confiantes e errados.

**A modelagem foi consequência das perguntas, não uma escolha de catálogo.** Não escolhi constelação de fatos por
achá-la elegante; cheguei nela porque três granularidades incompatíveis não cabiam em um fato só. O grão vem antes do
desenho, e o desenho vem das perguntas.

**Hipótese não é resultado — e a validação precisa vir da fonte.** Duas das minhas expectativas iniciais caíram
quando rodei o pipeline com todos os dados. O que me deu segurança para aceitar os números novos foi ter validado o
cálculo do corte contra a apuração oficial do ONS. Sem essa âncora, eu teria ficado tentado a desconfiar dos dados em
vez de desconfiar das minhas expectativas.

**A plataforma e a fonte não são neutras em relação à arquitetura.** Minha decisão de coletar por API era a
tecnicamente correta e colidiu com a restrição de rede da Free Edition; o layout da fonte mudou no meio do projeto.
Isso me obrigou a manter um plano B de ingestão e a construir um pipeline que se adapta — que é, provavelmente, a
lição mais próxima do trabalho real.

Por fim, escolher o setor elétrico não foi acaso. Lidar com dado de operação verificado — com sua revisão retroativa,
suas sentinelas e uma semântica que só o conhecimento do domínio explica — é o mesmo tipo de problema que encontro
fora da pós, em outro contexto.

---

## 8. Como reproduzir

### Pré-requisitos

- Conta na [Databricks Free Edition](https://www.databricks.com/learn/free-edition), com a verificação pelo LinkedIn
  concluída para liberar o acesso de saída à internet.
- Este repositório clonado no workspace como **Git folder** (`Workspace` → `Create` → `Git folder` → URL do
  repositório).

### Execução

Rode os notebooks **na ordem**, uma vez cada:

```
00_setup_ambiente      → cria catálogo, schemas e Volume; verifica licenças
01_coleta_bronze       → testa conectividade, baixa 1,15 GB da API do ONS e cria a Bronze (etapa mais longa)
02_silver_limpeza      → limpeza, tipagem, deduplicação e cálculo do corte
03_gold_modelagem      → dimensões, fatos, catálogo de dados, PK/FK e integridade
04_qualidade_dados     → verificações de qualidade
05_analise_perguntas   → responde P1 a P8
```

Se o teste de conectividade do notebook `01` falhar, a seção 2.1 do próprio notebook descreve o plano B: baixar os
mesmos arquivos e subi-los para o Volume, pulando as células de download.

### Ajustes possíveis

Em [`_config.py`](_config.py):

| Constante | Efeito |
|---|---|
| `CATALOG` | Nome do catálogo. Se a conta não permitir criar catálogos, usar `"workspace"` — o notebook `00` avisa |
| `ANO_INICIO` / `ANO_FIM` | Janela do balanço e do EAR |
| `COFF_INICIO` / `COFF_FIM` | Janela do constrained-off. Ampliar aumenta bastante o tempo de execução |

### Estrutura do repositório

```
MVP2/
├── README.md                        Este documento
├── _config.py                       Configuração compartilhada
├── 00_setup_ambiente.py             Catálogo, schemas, Volume, licenças
├── 01_coleta_bronze.py              Coleta via API → Volume → Bronze
├── 02_silver_limpeza.py             Bronze → Silver
├── 03_gold_modelagem.py             Silver → Gold (modelo dimensional)
├── 04_qualidade_dados.py            Qualidade de dados
├── 05_analise_perguntas.py          Análise — P1 a P8
└── docs/
    └── img/
        ├── p1_composicao_geracao.png … p8_usinas_taxa_corte.png   Gráficos da análise
        └── databricks/                                            Capturas da execução na plataforma
```

Os notebooks ficam na raiz porque todos precisam estar na mesma pasta para o `%run ./_config` funcionar.

---

## Créditos e licença

**Dados:** Portal de Dados Abertos do Operador Nacional do Sistema Elétrico (ONS) — https://dados.ons.org.br —
licenciados sob [Creative Commons Atribuição (CC-BY)](https://creativecommons.org/licenses/by/4.0/deed.pt_BR).

**Código:** material acadêmico, produzido por mim como MVP da disciplina de Engenharia de Dados.
