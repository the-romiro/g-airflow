# dbt — Melhorias

Migração da regra de negócio de Melhorias (Qlik → SQL + dbt) sobre SQL Server
`dbengenharia.dbo`. Plano e decisões em [`docs/`](docs/) (IDEIA, CONTEXT, ADRs).
Linhagem completa (sources → marts, com diagramas) em [`docs/lineage.md`](docs/lineage.md).

## Rodar

A credencial vem da Airflow Variable `ENG_DATABASE_URL` injetada como env vars
(ADR 0002). Localmente, exportar:

```bash
export DBT_HOST=... DBT_DB=dbengenharia DBT_USER=... DBT_PWD=... DBT_PORT=1433
dbt deps --profiles-dir .
dbt build --exclude resource_type:seed --profiles-dir .
```

O `dbt seed` foi descontinuado: as tabelas `mel_*` auxiliares são carregadas
manualmente no SQL Server (DBeaver/SQL), não via dbt. Por isso o build usa
`--exclude resource_type:seed`. Ver ADR 0004.

Todos os modelos são materializados como **VIEW** no schema `dbo` (limite de 10 GB,
import do Power BI — ADR 0001).

## Estrutura

- `models/staging/` — limpeza 1:1 sobre sources e tabelas auxiliares.
- `models/intermediate/` — regra de negócio (remap de gerente, cálculo de ganhos,
  unpivot/conversão R$, realizado, date spine).
- `models/marts/` — gold (dims + fatos), consumido pelo Power BI (import).
- `macros/generate_alias_name.sql` — prefixa toda relação criada pelo dbt com
  `mel_` (regra do projeto). No banco: `mel_stg_*`, `mel_int_*`, `mel_dim_*`,
  `mel_fct_*`. Os `ref()` continuam usando o node name (sem prefixo).
- `seeds/` — CSVs de mapeamento estático (`mel_bimestre`, `mel_categoria_melhorias`,
  `mel_gerente_remap`, `mel_gerente_area`), mantidos só como referência/contrato de
  schema. Não são mais carregados via `dbt seed`: as tabelas `dbo.mel_*` são populadas
  manualmente no SQL Server. Ver ADR 0004.

## Modelos

Tabela canônica (com upstream imediato e descrição por modelo) em
[`docs/lineage.md`](docs/lineage.md). Resumo das camadas:

| Camada | Modelos |
|---|---|
| staging | `stg_mel_aprovacao`, `stg_mel_ganhos`, `stg_meta_aderencia`, `stg_carga_horaria`, `stg_custo_funcionario` |
| intermediate | `int_ganhos_calc`, `int_ganhos_long`, `int_melhorias_realizado`, `int_gerente_remap`, `int_calendario_spine` |
| marts (dims) | `dim_calendario`, `dim_gerente`, `dim_idealizador`, `dim_tipo_melhoria` |
| marts (fatos) | `fct_aderencia`, `fct_ganhos`, `fct_meta`, `fct_melhorias_em_andamento` |

`stg_meta_aderencia` (sobre `mel_meta_aderencia`) é a tabela **central**, grão
`(bimestre, crachá)`: alimenta a meta (`fct_meta`), o roster (`dim_idealizador`),
o remap de gerente (`int_gerente_remap` → `dim_gerente`) e a aderência (`fct_aderencia`).

## Aderência (meta × realizado)

`fct_aderencia` cruza meta x realizado no grão `(bimestre, crachá)` e responde, por
idealizador/bimestre: quantas melhorias fez, atingiu a meta, ganhou quanto.

- **Realizado** (`int_melhorias_realizado`): `qtde_melhorias` conta FAPs distintas com
  `ganho_fap >= 0` E `replicacao = 'Não'` (melhoria = original, não replicação). Já o
  `ganho_total` soma **todos** os ganhos por FAP do idealizador/bimestre, independente
  de replicacao/sinal (não usa o filtro da contagem).
- **Meta**: roster elegível vindo de `stg_meta_aderencia` (meta fixa de 1 por elegível).

Bimestre canônico em todo o modelo: `'mes1-mes2/ano'` (ex. `jan-fev/2026`), derivado
de `dt_fap` (ganhos/aprovação) ou `inicio_bimestre` (meta) via seed `mel_bimestre`.

## Ganhos (cálculo R$)

`int_ganhos_calc` calcula os ganhos ainda WIDE (meses 1..4 em colunas) seguindo a regra
Qlik. `int_ganhos_long` faz o unpivot 1..4 e a conversão por tipo, gerando uma linha por
`(FAP, mês de competência)` consumida por `fct_ganhos`:

- **TC/Aglutinação**: R$ trabalhista (`hrs_ganhas / ch_mensal * custo`, competência = mês
  de `dt_fap`), via join com `stg_carga_horaria` + `stg_custo_funcionario`.
- **Consumo/Troca MP**: já em R$, somados.
- **Demais tipos**: `ganho_previsto` no mês 1.

## Collation

`mel_ganhos` (ingerido por DAG) é `Latin1_General_CI_AS`; tabelas de planilha/VBA
(`mel_carga_horaria`, `mel_custo_funcionario`) e as tabelas `mel_*` auxiliares são
`Latin1_General_100_CI_AS_SC_UTF8`. Os joins de string entre as duas origens em
`int_ganhos_long` usam `collate database_default` pra não estourar conflito. Ver ADR 0003.

## Pendências (TODO nos models)

- Conversão R$ TC em `int_ganhos_long`: validar as chaves de join (carga por
  `setor`+mês, custo por `filial`+mês) e os totais contra `[R$ Mês TC]` do Qlik.
- `mel_gerente_area`: popular a tabela no SQL Server com a planilha `gerentes`
  (CSV `seeds/mel_gerente_area.csv` serve de referência do schema).
