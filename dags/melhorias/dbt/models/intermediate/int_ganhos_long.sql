-- Ganhos LONG: uma linha por (FAP, mês 1..4) com ganho_reais convertido (ADR 0003).
-- Espelha base_melhoria do Qlik (docs/qlik/load_data_for_dashboard.md).
--
-- ganho_reais por tipo (regra confirmada):
--   TC/Aglutinação      -> hrs_ganhas_N / ch_mensal * custo  (conversão R$ trabalhista)
--   Consumo/Troca MP    -> g_consumo_N + troca_mp_N          (já em R$)
--   Outros tipos        -> ganho_previsto                    (atribuído ao mês 1)
--
-- Período da conversão = mês de dt_fap (Qlik usa a Data da FAP para os 4 meses).
-- Chaves (assunção a validar vs Qlik):
--   carga_horaria  por (setor = [Setor], mês de dt_fap)
--   custo_funcionario por (filial = [Estab.], mês de dt_fap)

with calc as (
    select * from {{ ref('int_ganhos_calc') }}
),

carga as (
    select
        setor,
        year(mes) as ano,
        month(mes) as mes_num,
        ch_mensal
    from {{ ref('stg_carga_horaria') }}
),

custo as (
    select
        estabelecimento,
        year(dt_competencia) as ano,
        month(dt_competencia) as mes_num,
        custo
    from {{ ref('stg_custo_funcionario') }}
),

calc_aux as (
    select
        c.*,
        ch.ch_mensal,
        cu.custo
    from calc as c
    -- COLLATE DATABASE_DEFAULT: mel_ganhos (setor/filial) e Latin1_General_CI_AS;
    -- carga/custo (planilha/VBA) sao UTF8. Sem isto o join de string estoura collation.
    left join carga as ch
        on c.setor collate database_default = ch.setor collate database_default
        and year(c.dt_fap) = ch.ano
        and month(c.dt_fap) = ch.mes_num
    left join custo as cu
        on c.filial collate database_default = cu.estabelecimento collate database_default
        and year(c.dt_fap) = cu.ano
        and month(c.dt_fap) = cu.mes_num
)

{% for n in range(1, 5) %}
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
    {{ n }} as mes_offset,
    dateadd(month, {{ n - 1 }}, dt_fap) as dt_competencia,
    hrs_ganhas_{{ n }} as hrs_ganhas,
    case
        when tipo_melhoria in ('Tempo Ciclo', 'Aglutinação')
            then case
                when ch_mensal is not null and ch_mensal <> 0
                    then hrs_ganhas_{{ n }} / ch_mensal * custo
                else 0
            end
        when tipo_melhoria in ('Consumo', 'Troca de Matéria Prima') or setor like 'Ting%'
            then g_consumo_{{ n }} + troca_mp_{{ n }}
        -- Só existe ganho_previsto para o primeiro mês (1).
        else case when {{ n }} = 1 then coalesce(ganho_previsto, 0) else 0 end
    end as ganho_reais
from calc_aux
where dt_fap is not null
{% if not loop.last %}union all{% endif %}
{% endfor %}
