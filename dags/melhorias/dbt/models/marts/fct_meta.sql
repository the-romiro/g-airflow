-- Fato de meta: alvo por gerente/bimestre (roster elegível). Grão distinto do
-- realizado (funcionário elegível x bimestre).

select
    bimestre,
    inicio_bimestre,
    fim_bimestre,
    gerente_meta,
    codigo as cracha,
    nome,
    cargo,
    grupo_cargo,
    qtde as meta_qtde,
    elegivel
from {{ ref('stg_meta_aderencia') }}
