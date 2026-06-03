-- Limpeza 1:1 sobre mel_meta_aderencia (meta por gerente/bimestre + roster RH).
-- Deriva flag elegivel (Qlik: Grupo Cargo <> 'Não Elegível' E Categoria = 'Considera').

select
    bimestre,
    inicio_bimestre,
    fim_bimestre,
    gerente_meta,
    gerente_c_custo,
    atende_somente_um_gerente,
    grupo_cargo,
    categoria_situacao,
    descricao_situacao,
    setor_pnope,
    codigo,
    nome,
    setor,
    c_custo,
    cargo,
    situacao,
    -- Meta fixa: cada elegível deve fazer 1 melhoria/bimestre (CONTEXT: "meta 1
    -- fez 1, meta ok"). mel_meta_aderencia não carrega quantidade.
    cast(1 as int) as qtde,
    case
        when grupo_cargo <> 'Não Elegível' and categoria_situacao = 'Considera'
            then cast(1 as bit)
        else cast(0 as bit)
    end as elegivel
from {{ source('aux', 'mel_meta_aderencia') }}
