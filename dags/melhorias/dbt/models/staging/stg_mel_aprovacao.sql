-- Limpeza 1:1 sobre o bronze mel_aprovacao.
-- Deriva dt_fap (antes feito via UPDATE in-place pela DAG SILVER_MELHORIAS_DATA_FAP):
-- quando aprovado e dt_fap nulo, usa dt_aprovacao_eng (ADR 0003: dt_fap = data mestra).
-- Bimestre canônico 'mes1-mes2/ano' derivado de dt_fap via seed mel_bimestre.

with aprovacao as (
    select
        id,
        numero_fap,
        numero_fap_pai,
        tipo_melhoria,
        tipo_fap,
        status,
        setor,
        gerente_melhoria,
        cracha_idealizador,
        nome_idealizador,
        cc_idealizador,
        gerente_idealizador,
        cargo_idealizador,
        dt_aprovacao_eng,
        dt_inicio_fluxo,
        eh_ganho,
        ganho_previsto = cast(null as decimal(18, 2)),  -- placeholder: aprovacao não tem ganho_previsto
        created,
        modified,
        -- ADR 0003: data mestra
        cast(
            coalesce(dt_fap, case when status = 'Aprovado' then dt_aprovacao_eng end) as date
        ) as dt_fap
    from {{ source('bronze', 'mel_aprovacao') }}
)

select
    a.*,
    case
        when a.dt_fap is not null
            then b.bimestre + '/' + cast(year(a.dt_fap) as varchar(4))
    end as bimestre
from aprovacao as a
left join {{ ref('mel_bimestre') }} as b
    on month(a.dt_fap) = b.mes
