-- Fato de aderência: meta (CENTRAL, roster elegível) x realizado de melhorias.
-- Grão: (bimestre, crachá idealizador). Responde, por idealizador/bimestre:
--   quantas melhorias fez (realizado_qtde), foi aderente (atingiu_meta),
--   ganhou quanto (ganho_total).

with meta as (
    select
        bimestre,
        codigo as cracha_idealizador,
        gerente_meta,
        qtde as meta_qtde,
        elegivel
    from {{ ref('stg_meta_aderencia') }}
    where elegivel = 1
),

realizado as (
    select * from {{ ref('int_melhorias_realizado') }}
)

select
    m.bimestre,
    m.cracha_idealizador,
    m.gerente_meta,
    m.meta_qtde,
    coalesce(r.qtde_melhorias, 0) as realizado_qtde,
    coalesce(r.ganho_total, 0) as ganho_total,
    case
        when m.meta_qtde > 0 and coalesce(r.qtde_melhorias, 0) >= m.meta_qtde
            then cast(1 as bit)
        else cast(0 as bit)
    end as atingiu_meta,
    m.elegivel
from meta as m
left join realizado as r
    on m.bimestre = r.bimestre
    and m.cracha_idealizador = r.cracha_idealizador
