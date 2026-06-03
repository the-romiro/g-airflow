-- Fato de ganhos LONG: uma linha por (FAP, mês de competência 1..4). ADR 0003.
-- ganho_reais (R$) convertido por tipo em int_ganhos_long. Chaveado em
-- (bimestre, crachá) para relacionar com a meta no Power BI.

select
    numero_fap,
    id_solicitacao_assinatura,
    tipo_melhoria,
    filial,
    setor,
    macro_setor,
    cracha_idealizador,
    nome_idealizador,
    gerente_idealizador,
    bimestre,
    replicacao,
    mes_offset,
    dt_competencia,
    hrs_ganhas,
    ganho_reais
from {{ ref('int_ganhos_long') }}
where dt_competencia is not null
