-- Pipeline: melhorias NÃO aprovadas (dt_fap nulo). ADR 0003: fora dos fatos de
-- aprovadas. Referenciada por created (não dt_fap). Carrega status do fluxo.

select
    id,
    numero_fap,
    tipo_melhoria,
    status,
    setor,
    gerente_idealizador,
    cracha_idealizador,
    nome_idealizador,
    created as dt_referencia,
    dt_inicio_fluxo
from {{ ref('stg_mel_aprovacao') }}
where dt_fap is null
