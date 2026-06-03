-- Limpeza 1:1 sobre o bronze mel_ganhos.
-- mel_ganhos tem dt_fap nativo (DDL) -> bimestre sai direto daqui, sem join com
-- aprovacao. Bimestre canônico 'mes1-mes2/ano' via seed mel_bimestre (month -> bimestre).

with ganhos as (
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
        cast(dt_fap as date) as dt_fap,
        created,
        modified
    from {{ source('bronze', 'mel_ganhos') }}
)

select
    g.*,
    -- bimestre canônico (ADR 0003): mel_bimestre.bimestre + '/' + ano de dt_fap
    case
        when g.dt_fap is not null
            then b.bimestre + '/' + cast(year(g.dt_fap) as varchar(4))
    end as bimestre
from ganhos as g
left join {{ ref('mel_bimestre') }} as b
    on month(g.dt_fap) = b.mes
