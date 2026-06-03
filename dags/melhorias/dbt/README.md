# dbt — Melhorias

Migração da regra de negócio de Melhorias (Qlik → SQL + dbt) sobre SQL Server
`dbengenharia.dbo`. Plano e decisões em [`docs/`](docs/) (IDEIA, CONTEXT, ADRs).

## Rodar

A credencial vem da Airflow Variable `ENG_DATABASE_URL` injetada como env vars
(ADR 0002). Localmente, exportar:

```bash
export DBT_HOST=... DBT_DB=dbengenharia DBT_USER=... DBT_PWD=... DBT_PORT=1433
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt build --profiles-dir .
```

## Estrutura

- `models/staging/` — limpeza 1:1 sobre sources/seeds (view).
- `models/intermediate/` — regra de negócio (remap, cálculo de ganhos, realizado).
- `models/marts/` — gold (dims + fatos), consumido pelo Power BI (import).
- `macros/generate_alias_name.sql` — prefixa toda relação criada pelo dbt com
  `mel_` (regra do projeto). No banco: `mel_stg_*`, `mel_int_*`, `mel_dim_*`,
  `mel_fct_*`. Os `ref()` continuam usando o node name (sem prefixo).
- `seeds/` — mapeamentos estáticos, tabelas com prefixo `mel_` (`mel_gerente_remap`,
  `mel_gerente_area`, `mel_categoria_melhorias`, `mel_bimestre`).

## Modelo central (meta_aderencia)

`mel_meta_aderencia` é a tabela central (grão `bimestre, crachá`). O realizado de
melhorias vem dos ganhos: **melhoria = `numero_fap` com `ganho_fap >= 0` E
`replicacao = 'Não'`** (`int_melhorias_realizado`). Já o `ganho_total` soma **todos**
os ganhos por FAP do idealizador/bimestre, independente de replicacao/sinal (não usa o
filtro da contagem). `fct_aderencia` cruza meta x realizado e responde, por
idealizador/bimestre: quantas melhorias fez, foi aderente, ganhou quanto.

Bimestre canônico em todo o modelo: `'mes1-mes2/ano'` (ex. `jan-fev/2026`), derivado
de `dt_fap` (ganhos/aprovacao) ou `inicio_bimestre` (meta) via seed `mel_bimestre`.

`int_ganhos_long` faz o unpivot 1..4 e a conversão de ganho por tipo: TC/Aglutinação
via R$ trabalhista (`hrs_ganhas / ch_mensal * custo`, período = mês de `dt_fap`),
Consumo/Troca MP somados, demais tipos = `ganho_previsto`.

## Collation

`mel_ganhos` (ingerido por DAG) é `Latin1_General_CI_AS`; tabelas de planilha/VBA
(`mel_carga_horaria`, `mel_custo_funcionario`) e seeds são
`Latin1_General_100_CI_AS_SC_UTF8`. Os joins de string entre as duas origens em
`int_ganhos_long` usam `collate database_default` pra não estourar conflito. Ver ADR 0003.

## Pendências (TODO nos models)

- Conversão R$ TC em `int_ganhos_long`: validar as chaves de join (carga por
  `macro_setor`+mês, custo por `filial`+mês) e os totais contra `[R$ Mês TC]` do Qlik.
- `seeds/mel_gerente_area.csv`: preencher com a planilha `gerentes`.
- Dep `dbt-sqlserver` + driver ODBC no Dockerfile (ver pyproject grupo `dbt`).
