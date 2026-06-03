-- Calculo de ganhos (ainda WIDE: meses 1..4 em colunas). O unpivot para long
-- acontece em fct_ganhos (ADR 0003). Espelha base_melhoria do Qlik.
--
-- Tipos: TC/Aglutinação -> horas ganhas; Consumo -> economia MP; Troca MP -> custo par.
-- Regra Qlik: setor like 'Ting%' cai em troca MP mesmo sendo Consumo.
--
-- TODO: conversão R$ trabalhista (ganho_reais_N = hrs_ganhas_N / hrs_trabalhadas_mes
--       * custo_funcionario) precisa do join com stg_carga_horaria (setor, mês) e
--       stg_custo_funcionario (estab, mês). Chaves de período/estab a confirmar.

with ganhos as (
    select
        g.*,
        a.dt_fap  -- ADR 0003: dt_fap dos ganhos vem da aprovacao (cross-table)
    from {{ ref('stg_mel_ganhos') }} as g
    left join {{ ref('stg_mel_aprovacao') }} as a
        on g.id_solicitacao_assinatura = a.id
),

componentes as (
    select
        numero_fap,
        id_solicitacao_assinatura,
        tipo_melhoria,
        filial,
        setor,
        macro_setor,
        cracha_idealizador,
        nome_idealizador,
        dt_fap,
        -- flag de classe (segue Qlik: Ting% sempre troca MP)
        case
            when tipo_melhoria in ('Consumo', 'Troca de Matéria Prima') then 0 else 1
        end as eh_tempo_ciclo,
        {% for n in range(1, 5) %}
        -- mês {{ n }}
        case
            when tipo_melhoria not in ('Consumo', 'Troca de Matéria Prima')
                then (tc_anterior - tc_atual) * (mix / 100.0) * volume_mes_{{ n }} / 60.0
            else 0
        end as hrs_ganhas_{{ n }},
        case
            when tipo_melhoria = 'Consumo' and setor not like 'Ting%'
                then (consumo_anterior - consumo_atual) * (mix / 100.0)
                    * preco_mp * volume_mes_{{ n }}
            else 0
        end as g_consumo_{{ n }},
        case
            when tipo_melhoria = 'Troca de Matéria Prima' or setor like 'Ting%'
                then (custo_par_anterior - custo_par_atual) * (mix / 100.0)
                    * volume_mes_{{ n }}
            else 0
        end as troca_mp_{{ n }}{{ "," if not loop.last }}
        {% endfor %}
    from ganhos
)

select * from componentes
