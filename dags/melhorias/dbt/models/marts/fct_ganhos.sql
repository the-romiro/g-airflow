-- Fato de ganhos LONG: uma linha por (FAP, mês de competência 1..4). ADR 0003.
-- Unpivot dos _1.._4 de int_ganhos_calc; dt_competencia = dt_fap + (offset-1) meses.

with calc as (
    select * from {{ ref('int_ganhos_calc') }}
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
    {{ n }} as mes_offset,
    dateadd(month, {{ n - 1 }}, dt_fap) as dt_competencia,
    hrs_ganhas_{{ n }} as hrs_ganhas,
    g_consumo_{{ n }} as ganho_consumo,
    troca_mp_{{ n }} as ganho_troca_mp
    -- TODO: ganho_reais, ganho_pessoas (precisam aux: carga_horaria + custo_funcionario)
from calc
where dt_fap is not null
{% if not loop.last %}union all{% endif %}
{% endfor %}
