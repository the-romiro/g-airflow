MERGE dbengenharia.dbo.mel_ganhos AS t
USING dbengenharia.dbo.stg_mel_ganhos AS s
ON (t.id = s.id)

WHEN MATCHED
  THEN
  UPDATE
    SET
      t.tipo_melhoria = s.tipo_melhoria
    , t.filial = s.filial
    , t.fabrica = s.fabrica
    , t.setor = s.setor
    , t.nome_solicitante = s.nome_solicitante
    , t.numero_fap = s.numero_fap2
    , t.dt_fap = CAST(s.dt_fap AS DATE)
    , t.cod_prod = s.cod_prod
    , t.preco_mp = s.preco_mp
    , t.consumo_anterior = s.consumo_anterior
    , t.consumo_atual = s.consumo_atual
    , t.padrao_anterior = s.padrao_anterior
    , t.padrao_atual = s.padrao_atual
    , t.efetivo_anterior = s.efetivo_anterior
    , t.efetivo_atual = s.efetivo_atual
    , t.tc_anterior = s.tc_anterior
    , t.tc_atual = s.tc_atual
    , t.mix = s.mix
    , t.volume_mes_1 = s.volume_mes_1
    , t.volume_mes_2 = s.volume_mes_2
    , t.volume_mes_3 = s.volume_mes_3
    , t.volume_mes_4 = s.volume_mes_4
    , t.percentual_ganho = s.[_x0025__ganho]
    , t.custo_par_anterior = s.custo_par_anterior
    , t.custo_par_atual = s.custo_par_atual
    , t.origem_melhoria = s.origem_melhoria
    , t.macro_setor = s.macro_setor
    , t.replicavel = s.replicavel
    , t.nome_setor_replicavel = s.nome_setor_replicavel
    , t.is_lacamento_manual = s.is_lacamento_manual
    , t.investimento = s.investimento
    , t.cracha_idealizador = s.cracha_idealizador
    , t.descricao_proc_atual_outras_m = s.descricao_proc_atual_outras_m
    , t.descricao_proc_prop_outras_m = s.descricao_proc_prop_outras_m
    , t.qtde_mo_atual_outras_m = s.qtde_mo_atual_outras_m
    , t.qtde_mo_proposto_outras_m = s.qtde_mo_proposto_outras_m
    , t.dt_fap_informada_por = s.dt_fap_informada_por
    , t.custo_mod = s.salario_mod
    , t.carga_horaria_mes = s.carga_horaria_mes
    , t.peso_ponderado_componente = s.peso_ponderado_componente
    , t.ganho_previsto = s.ganho_previsto
    , t.tipo_alteracao = s.tipo_alteracao
    , t.id_solicitacao_assinatura = s.id_solicitacao_assinatura
    , t.gerente = s.gerente
    , t.tipo_fap = s.tipo_fap
    , t.nome_idealizador = s.nome_idealizador
    , t.cc_idealizador = s.cc_idealizador
    , t.gerente_idealizador = s.gerente_x0020_idealizador
    , t.created = s.created
    , t.modified = s.modified
    , t.replicacao = s.[_x00c9__x0020_uma_x0020_replica_]
    , t.volume_foi_editado = s.volume_x0020_foi_x0020_editado
    , t.cargo = s.cargo
    , t.status_liberacao = CAST(s.status_x0020_de_x0020_libera_x00 AS VARCHAR)
    , t.tipo_produto = s.tipo_produto
    , t.author = s.author
    , t.editor = s.editor

WHEN NOT MATCHED
  THEN
  INSERT (
    id, tipo_melhoria, filial, fabrica, setor, nome_solicitante, numero_fap, dt_fap
  , cod_prod, preco_mp, consumo_anterior, consumo_atual, padrao_anterior, padrao_atual
  , efetivo_anterior, efetivo_atual, tc_anterior, tc_atual, mix, volume_mes_1
  , volume_mes_2, volume_mes_3, volume_mes_4, percentual_ganho, custo_par_anterior
  , custo_par_atual, origem_melhoria, macro_setor, replicavel, nome_setor_replicavel
  , is_lacamento_manual, investimento, cracha_idealizador, descricao_proc_atual_outras_m
  , descricao_proc_prop_outras_m, qtde_mo_atual_outras_m, qtde_mo_proposto_outras_m
  , dt_fap_informada_por, custo_mod, carga_horaria_mes, peso_ponderado_componente
  , ganho_previsto, tipo_alteracao, id_solicitacao_assinatura, gerente, tipo_fap
  , nome_idealizador, cc_idealizador, gerente_idealizador, created, modified
  , replicacao, volume_foi_editado, cargo, status_liberacao, tipo_produto, author, editor
  )
  VALUES (
    s.id
  , s.tipo_melhoria
  , s.filial
  , s.fabrica
  , s.setor
  , s.nome_solicitante
  , s.numero_fap2
  , CAST(s.dt_fap AS DATE)
  , s.cod_prod, s.preco_mp, s.consumo_anterior, s.consumo_atual, s.padrao_anterior, s.padrao_atual
  , s.efetivo_anterior, s.efetivo_atual, s.tc_anterior, s.tc_atual, s.mix, s.volume_mes_1
  , s.volume_mes_2, s.volume_mes_3, s.volume_mes_4, s.[_x0025__ganho], s.custo_par_anterior
  , s.custo_par_atual, s.origem_melhoria, s.macro_setor, s.replicavel, s.nome_setor_replicavel
  , s.is_lacamento_manual, s.investimento, s.cracha_idealizador, s.descricao_proc_atual_outras_m
  , s.descricao_proc_prop_outras_m, s.qtde_mo_atual_outras_m, s.qtde_mo_proposto_outras_m
  , s.dt_fap_informada_por, s.salario_mod, s.carga_horaria_mes, s.peso_ponderado_componente
  , s.ganho_previsto, s.tipo_alteracao, s.id_solicitacao_assinatura, s.gerente, s.tipo_fap
  , s.nome_idealizador, s.cc_idealizador, s.gerente_x0020_idealizador, s.created, s.modified
  , s.[_x00c9__x0020_uma_x0020_replica_], s.volume_x0020_foi_x0020_editado, s.cargo
  , CAST(s.status_x0020_de_x0020_libera_x00 AS VARCHAR), s.tipo_produto, s.author, s.editor
  )

-- DELETE
WHEN NOT MATCHED BY SOURCE
  THEN DELETE;
