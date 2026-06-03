-- Limpeza 1:1 sobre o bronze mel_ganhos.
-- dt_fap dos ganhos depende de join com aprovacao (cross-table) -> resolvido em
-- int_ganhos_calc, não aqui (staging é single-source). ADR 0003.

select
    id,
    id_solicitacao_assinatura,
    numero_fap,
    tipo_melhoria,
    filial,
    fabrica,
    setor,
    macro_setor,
    cod_prod,
    cracha_idealizador,
    nome_idealizador,
    cc_idealizador,
    gerente,
    gerente_idealizador,
    preco_mp,
    consumo_anterior,
    consumo_atual,
    tc_anterior,
    tc_atual,
    custo_par_anterior,
    custo_par_atual,
    mix,
    volume_mes_1,
    volume_mes_2,
    volume_mes_3,
    volume_mes_4,
    ganho_previsto,
    investimento,
    replicacao,
    cargo,
    created,
    modified
from {{ source('bronze', 'mel_ganhos') }}
