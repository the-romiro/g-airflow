-- Realizado de aderência: melhorias aprovadas por (bimestre, crachá idealizador).
-- ADR 0003: só aprovadas (dt_fap não nulo). Bimestre derivado de dt_fap via seed.
--
-- TODO: confirmar contagem do Qlik ("Qtde de Melhorias" vs "Qtde Pessoas com
--       melhorias") e o join com int_gerente_remap pelo gerente do idealizador.

with aprovadas as (
    select
        cracha_idealizador,
        nome_idealizador,
        gerente_idealizador,
        dt_fap,
        cast(year(dt_fap) as varchar(4)) + '_Bimestre_'
            + cast(((month(dt_fap) + 1) / 2) as varchar(2)) as bimestre
    from {{ ref('stg_mel_aprovacao') }}
    where dt_fap is not null
)

select
    bimestre,
    cracha_idealizador,
    nome_idealizador,
    gerente_idealizador,
    count(*) as qtde_melhorias
from aprovadas
group by
    bimestre,
    cracha_idealizador,
    nome_idealizador,
    gerente_idealizador
