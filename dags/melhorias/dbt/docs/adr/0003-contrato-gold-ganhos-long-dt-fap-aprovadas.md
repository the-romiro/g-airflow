# 0003 — Contrato gold: ganhos long, dt_fap mestra, só aprovadas

- Status: aceito
- Data: 2026-06-02

## Contexto

O Qlik (`base_melhoria`) é WIDE — o ganho quadrimestral vive em colunas paralelas
(`hrs ganhas 1..4`, `g consumo 1..4`, `ganho troca MP 1..4`, `ganho em reais 1..4`).
A data de calendário no Qlik era espalhada ("Data da FAP" em ganhos, "Data para
Meta" em aderência), e melhorias em andamento eram jogadas no mês atual via
`Today()` pra contar no painel. O consumidor agora é Power BI (import, modo estrela).

## Decisão

1. **`fct_ganhos` long/unpivot** — grão = (FAP × mês de competência 1..4). Uma linha
   por mês, `dt_competencia = dt_fap + offset`. Os `_1.._4` do Qlik viram linhas.
   Liga `dim_calendario` por `dt_competencia`; aditivo no Power BI.
2. **`dt_fap` = data mestra única** — deriva de `dt_aprovacao_eng` (`status='Aprovado'`).
   Único link temporal dos fatos com `dim_calendario`. Unifica "Data da FAP" e
   "Data para Meta" do Qlik.
3. **Só aprovadas contam** — `dt_fap` null → fora de `fct_ganhos`/`fct_aderencia`.
   Melhorias em andamento vão num fato próprio `fct_melhorias_em_andamento`
   (referenciado por `Criado`, com `status` do fluxo + `ganho_previsto`). KPI
   principal não mistura realizado com pendente; gold não depende de `Today()` e
   reconstrói histórico estável.

## Consequências

- Diverge do Qlik de propósito: ele inflava o mês corrente com pendentes; aqui não.
- `dim_calendario` = date spine determinístico de `min(dt_fap)` a `max(dt_fap)`.
- A conversão R$ trabalhista (`ganho em reais`) usa `carga_horaria` + `custo_funcionario`
  do mês-base da FAP pros 4 offsets (igual Qlik); calcula em `int_ganhos_calc` antes
  do unpivot.
- Quem comparar dashboard novo vs Qlik vai ver diferença no mês corrente (pendentes).

## Alternativas consideradas

- **`fct_ganhos` wide (1 linha/FAP)**: espelha Qlik/planilha, mas Power BI só linka
  calendário por `dt_fap` base; fatiar por mês de competência exigiria unpivot em DAX.
- **Replicar em andamento no mês atual** (opção B do grilling): mais fiel, mas mistura
  realizado + previsão e amarra o gold à data de execução.
