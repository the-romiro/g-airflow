-- Cálculo de ganhos (ainda WIDE: meses 1..4 em colunas). O unpivot para long e a
-- conversão R$ acontecem em int_ganhos_long (ADR 0003). Espelha base_melhoria do Qlik.
--
-- Tipos: TC/Aglutinação -> horas ganhas; Consumo -> economia MP; Troca MP -> custo par.
-- Regra Qlik: setor like 'Ting%' cai em troca MP mesmo sendo Consumo.
--
-- dt_fap e bimestre vêm de stg_mel_ganhos (mel_ganhos tem dt_fap nativo) -> sem join
-- com aprovacao. ganho_reais (R$) é resolvido no long (precisa de carga/custo por mês).

with ganhos as (
    select * from {{ ref('stg_mel_ganhos') }}
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
        gerente_idealizador,
        dt_fap,
        bimestre,
        replicacao,
        ganho_previsto,
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
