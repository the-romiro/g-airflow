-- Realizado de melhorias por (bimestre, crachá idealizador). Tabela central = meta;
-- este é o realizado que entra no fct_aderencia.
--
-- Definição de melhoria (regra confirmada): numero_fap com ganho_reais > 0 (somado
-- nos 4 meses) E replicacao = 'Não'. Replicações e FAPs sem ganho não contam.
-- ganho_total = soma do ganho_reais dos FAPs que contam, no bimestre/idealizador.

with por_fap as (
    select
        bimestre,
        cracha_idealizador,
        numero_fap,
        max(nome_idealizador) as nome_idealizador,
        max(gerente_idealizador) as gerente_idealizador,
        max(replicacao) as replicacao,
        sum(ganho_reais) as ganho_fap
    from {{ ref('int_ganhos_long') }}
    where bimestre is not null
    group by bimestre, cracha_idealizador, numero_fap
),

melhorias as (
    select *
    from por_fap
    where ganho_fap > 0 and replicacao = 'Não'
)

select
    bimestre,
    cracha_idealizador,
    max(nome_idealizador) as nome_idealizador,
    max(gerente_idealizador) as gerente_idealizador,
    count(distinct numero_fap) as qtde_melhorias,
    sum(ganho_fap) as ganho_total
from melhorias
group by bimestre, cracha_idealizador
