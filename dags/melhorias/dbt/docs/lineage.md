# Linhagem de dados — dbt melhorias

Fluxo de dados do projeto dbt `melhorias`, da origem (SQL Server `dbengenharia`) até os
fatos e dimensões de consumo do Power BI. Arquitetura medallion:
**Sources → Staging → Intermediate → Marts**.

A linhagem aqui é derivada do `target/manifest.json` (gerado no build do dbt) — fonte
canônica das dependências `ref()`/`source()`. Para regenerar após mudanças nos modelos,
rode o dbt (build/compile) e atualize este documento conforme o novo manifest.

> ADR 0001: todos os modelos são materializados como **VIEW** no schema `dbo`
> (limite de 10 GB, import do Power BI). Seeds também vivem em `dbo`.

## Diagrama global

```mermaid
flowchart LR
  classDef src fill:#e8d4b0,color:#000;
  classDef seed fill:#d4e8b0,color:#000;
  classDef stg fill:#cd7f32,color:#fff;
  classDef int fill:#c0c0c0,color:#000;
  classDef mart fill:#ffd700,color:#000;

  subgraph Sources["Sources (dbengenharia.dbo)"]
    src_aprov[bronze.mel_aprovacao]:::src
    src_ganhos[bronze.mel_ganhos]:::src
    src_meta[aux.mel_meta_aderencia]:::src
    src_carga[aux.mel_carga_horaria]:::src
    src_custo[aux.mel_custo_funcionario]:::src
  end
  subgraph Seeds
    sd_bim[mel_bimestre]:::seed
    sd_cat[mel_categoria_melhorias]:::seed
    sd_remap[mel_gerente_remap]:::seed
    sd_area[mel_gerente_area]:::seed
  end
  subgraph Staging
    stg_aprov[stg_mel_aprovacao]:::stg
    stg_ganhos[stg_mel_ganhos]:::stg
    stg_meta[stg_meta_aderencia]:::stg
    stg_carga[stg_carga_horaria]:::stg
    stg_custo[stg_custo_funcionario]:::stg
  end
  subgraph Intermediate
    int_calc[int_ganhos_calc]:::int
    int_long[int_ganhos_long]:::int
    int_spine[int_calendario_spine]:::int
    int_remap[int_gerente_remap]:::int
    int_real[int_melhorias_realizado]:::int
  end
  subgraph Marts
    d_tipo[dim_tipo_melhoria]:::mart
    d_cal[dim_calendario]:::mart
    d_ger[dim_gerente]:::mart
    d_ideal[dim_idealizador]:::mart
    f_ganhos[fct_ganhos]:::mart
    f_ader[fct_aderencia]:::mart
    f_meta[fct_meta]:::mart
    f_andamento[fct_melhorias_em_andamento]:::mart
  end

  src_aprov --> stg_aprov
  src_ganhos --> stg_ganhos
  src_meta --> stg_meta
  src_carga --> stg_carga
  src_custo --> stg_custo
  sd_bim --> stg_aprov & stg_meta & stg_ganhos & d_cal
  sd_cat --> d_tipo
  sd_remap --> int_remap
  sd_area --> int_remap
  stg_meta --> int_remap & f_ader & d_ideal & f_meta
  stg_aprov --> f_andamento
  stg_ganhos --> int_calc
  stg_carga --> int_long
  stg_custo --> int_long
  int_calc --> int_long
  int_long --> f_ganhos & int_real
  int_spine --> d_cal
  int_remap --> d_ger
  int_real --> f_ader
```

## Diagramas focados por fato

Subgrafos de impacto: o que muda quando se altera um upstream de cada fato.

### fct_aderencia — meta × realizado

Grão (bimestre, crachá idealizador). Cruza a meta (roster elegível) com o realizado de
melhorias por idealizador.

```mermaid
flowchart LR
  src_meta[aux.mel_meta_aderencia] --> stg_meta[stg_meta_aderencia]
  src_ganhos[bronze.mel_ganhos] --> stg_ganhos[stg_mel_ganhos]
  sd_bim[mel_bimestre] --> stg_meta & stg_ganhos
  stg_carga[stg_carga_horaria] --> int_long[int_ganhos_long]
  stg_custo[stg_custo_funcionario] --> int_long
  stg_ganhos --> int_calc[int_ganhos_calc] --> int_long
  int_long --> int_real[int_melhorias_realizado]
  stg_meta --> f_ader[fct_aderencia]
  int_real --> f_ader
```

### fct_ganhos — caminho de ganhos LONG

Uma linha por (FAP, mês de competência 1..4). Conversão para R$ por tipo em
`int_ganhos_long` (ADR 0003).

```mermaid
flowchart LR
  src_ganhos[bronze.mel_ganhos] --> stg_ganhos[stg_mel_ganhos]
  sd_bim[mel_bimestre] --> stg_ganhos
  stg_ganhos --> int_calc[int_ganhos_calc] --> int_long[int_ganhos_long]
  stg_carga[stg_carga_horaria] --> int_long
  stg_custo[stg_custo_funcionario] --> int_long
  int_long --> f_ganhos[fct_ganhos]
```

### fct_meta / dim_idealizador — roster da meta

Ambos saem direto do roster da meta (`stg_meta_aderencia`, tabela central).

```mermaid
flowchart LR
  src_meta[aux.mel_meta_aderencia] --> stg_meta[stg_meta_aderencia]
  sd_bim[mel_bimestre] --> stg_meta
  stg_meta --> f_meta[fct_meta]
  stg_meta --> d_ideal[dim_idealizador]
```

### fct_melhorias_em_andamento — pipeline não aprovado

Melhorias ainda sem `dt_fap` (não aprovadas). Fora dos fatos de aprovadas (ADR 0003).

```mermaid
flowchart LR
  src_aprov[bronze.mel_aprovacao] --> stg_aprov[stg_mel_aprovacao]
  sd_bim[mel_bimestre] --> stg_aprov
  stg_aprov --> f_andamento[fct_melhorias_em_andamento]
```

### dim_gerente — remap + área

```mermaid
flowchart LR
  src_meta[aux.mel_meta_aderencia] --> stg_meta[stg_meta_aderencia]
  sd_remap[mel_gerente_remap] --> int_remap[int_gerente_remap]
  sd_area[mel_gerente_area] --> int_remap
  stg_meta --> int_remap
  int_remap --> d_ger[dim_gerente]
```

## Modelos

Todos materializados como `view` em `dbo`.

| Modelo | Camada | Upstream imediato | Descrição |
|---|---|---|---|
| stg_mel_aprovacao | staging | bronze.mel_aprovacao, mel_bimestre | Limpeza 1:1 do bronze; deriva `dt_fap` (aprovado e nulo → `dt_aprovacao_eng`, ADR 0003) e bimestre canônico. |
| stg_mel_ganhos | staging | bronze.mel_ganhos, mel_bimestre | Limpeza 1:1 do bronze; `dt_fap` nativo → bimestre direto, sem join com aprovação. |
| stg_meta_aderencia | staging | aux.mel_meta_aderencia, mel_bimestre | Limpeza 1:1; tabela CENTRAL — grão (bimestre, crachá). Deriva flag `elegivel` e bimestre canônico. |
| stg_carga_horaria | staging | aux.mel_carga_horaria | Limpeza 1:1 das horas trabalhadas mensais por setor. |
| stg_custo_funcionario | staging | aux.mel_custo_funcionario | Limpeza 1:1 do custo mensal por estabelecimento. |
| int_ganhos_calc | intermediate | stg_mel_ganhos | Cálculo de ganhos ainda WIDE (meses 1..4 em colunas). Espelha `base_melhoria` do Qlik. |
| int_ganhos_long | intermediate | int_ganhos_calc, stg_carga_horaria, stg_custo_funcionario | Ganhos LONG: uma linha por (FAP, mês 1..4) com `ganho_reais` convertido por tipo (ADR 0003). |
| int_calendario_spine | intermediate | — | Date spine manual (compatível com SQL Server 2008; evita WITH aninhado do dbt_utils). |
| int_gerente_remap | intermediate | stg_meta_aderencia, mel_gerente_remap, mel_gerente_area | Remap de gerente (janela de data) + lookup de área; fall-through mantém gerente original. |
| int_melhorias_realizado | intermediate | int_ganhos_long | Realizado por (bimestre, crachá idealizador): FAP com `ganho_fap >= 0`. Entra no fct_aderencia. |
| dim_tipo_melhoria | mart | mel_categoria_melhorias | Dimensão de tipo de melhoria + categoria. |
| dim_calendario | mart | int_calendario_spine, mel_bimestre | Calendário diário; link dos fatos por `dt_fap`/`dt_competencia` (ADR 0003). |
| dim_gerente | mart | int_gerente_remap | Dimensão de gerente já remapeado + área. |
| dim_idealizador | mart | stg_meta_aderencia | Dimensão de idealizador; grão (bimestre, crachá) — atributos variam por bimestre. |
| fct_ganhos | mart | int_ganhos_long | Fato de ganhos LONG por (FAP, mês 1..4); `ganho_reais` em R$. Chaveado em (bimestre, crachá). |
| fct_aderencia | mart | stg_meta_aderencia, int_melhorias_realizado | Fato de aderência: meta × realizado. Grão (bimestre, crachá idealizador). |
| fct_meta | mart | stg_meta_aderencia | Fato de meta: alvo por gerente/bimestre (roster elegível). |
| fct_melhorias_em_andamento | mart | stg_mel_aprovacao | Pipeline de melhorias NÃO aprovadas (`dt_fap` nulo). Referenciada por `created` (ADR 0003). |

## Sources

`bronze` e `aux` são **source names do dbt** (agrupamento lógico por origem do dado),
**não schemas** — todas as tabelas vivem no schema físico `dbengenharia.dbo`. A notação
`bronze.mel_ganhos` segue a convenção dbt `source_name.tabela` (= `source('bronze',
'mel_ganhos')`), que resolve para `dbengenharia.dbo.mel_ganhos`. `bronze` = ingerido das
listas SharePoint pelas DAGs Airflow; `aux` = populado via planilha + VBA (sem DAG).
Ver ADR 0002 (conexão eng via env var).

| Source | Relation | Descrição |
|---|---|---|
| bronze.mel_aprovacao | `dbengenharia.dbo.mel_aprovacao` | Fluxo de aprovação/assinatura das melhorias (FAP/FAO). |
| bronze.mel_ganhos | `dbengenharia.dbo.mel_ganhos` | Ganhos das melhorias (TC, consumo, troca MP, outros). |
| aux.mel_meta_aderencia | `dbengenharia.dbo.mel_meta_aderencia` | Meta por gerente/bimestre + roster RH elegível. |
| aux.mel_carga_horaria | `dbengenharia.dbo.mel_carga_horaria` | Carga horária mensal por setor. |
| aux.mel_custo_funcionario | `dbengenharia.dbo.mel_custo_funcionario` | Custo do funcionário mensal por estabelecimento. |

## Seeds

Carregados manualmente no SQL Server (DBeaver/SQL), não via `dbt seed`. Os CSVs em
`seeds/` ficam só como referência/contrato de schema; os `ref()` resolvem para as
tabelas `dbo.mel_*` criadas à mão. Ver ADR 0004.

| Seed | Descrição |
|---|---|
| mel_bimestre | Mapa mês (1-12) → bimestre (jan-fev .. nov-dez). |
| mel_categoria_melhorias | Mapa Tipo de Melhoria → Categoria (planilha). |
| mel_gerente_remap | Remap de gerente (cadeia `if` do Qlik): origem → destino, com `valido_ate` opcional. |
| mel_gerente_area | Lookup de área pelo nome do gerente já ajustado (planilha gerentes). |
