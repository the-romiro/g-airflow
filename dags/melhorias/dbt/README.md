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

## Pendências (TODO nos models)

- `int_ganhos_calc` / `fct_ganhos`: conversão R$ trabalhista (precisa
  `carga_horaria` + `custo_funcionario`; chaves de período/estab a confirmar).
- `int_aderencia_realizado`: confirmar regra de contagem vs Qlik.
- `seeds/mel_gerente_area.csv`: preencher com a planilha `gerentes`.
- Dep `dbt-sqlserver` + driver ODBC no Dockerfile (ver pyproject grupo `dbt`).
