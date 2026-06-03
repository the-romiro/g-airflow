-- Fato de aderência: realizado (int_aderencia_realizado) x meta (stg_meta_aderencia).
-- Grão: (bimestre, crachá idealizador). ADR 0003: só aprovadas.

with realizado as (
    select * from {{ ref('int_aderencia_realizado') }}
),

meta as (
    select
        bimestre,
        codigo as cracha_idealizador,
        gerente_meta,
        qtde as meta_qtde,
        elegivel
    from {{ ref('stg_meta_aderencia') }}
)

select
    m.bimestre,
    m.cracha_idealizador,
    m.gerente_meta,
    m.meta_qtde,
    m.elegivel,
    coalesce(r.qtde_melhorias, 0) as realizado_qtde,
    case
        when m.meta_qtde > 0 and coalesce(r.qtde_melhorias, 0) >= m.meta_qtde
            then cast(1 as bit)
        else cast(0 as bit)
    end as atingiu_meta
from meta as m
left join realizado as r
    on m.bimestre = r.bimestre
    and m.cracha_idealizador = r.cracha_idealizador
where m.elegivel = 1
