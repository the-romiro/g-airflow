MERGE [dbengenharia].[dbo].[mel_aprovacao] AS T
USING [dbengenharia].[dbo].[stg_mel_aprovacao] AS S
ON T.Id = S.Id

-- UPDATE (incremental)
WHEN MATCHED
AND (
  T.Modified <> S.Modified
  OR T.Modified IS NULL AND S.Modified IS NOT NULL
  OR T.Modified IS NOT NULL AND S.Modified IS NULL
)
  THEN UPDATE SET
    Fabrica_melhoria = S.Fabrica_melhoria
  , Filial_melhoria = S.Filial_melhoria
  , Origem_melhoria = S.Origem_melhoria
  , Macro_setor_nome = S.Macro_setor
  , Tipo_alteracao = S.Tipo_alteracao
  , Author = S.Author
  , Editor = S.Editor
  , numero_fap = S.Title
  , Email_aprovador_eng = S.Email_aprovador_eng
  , Dt_aprovacao_eng = S.Dt_aprovacao_eng
  , Comentarios_especialista = S.Comentarios_especialista
  , Email_aprovador_producao = S.Email_aprovador_producao
  , Dt_aprovacao_producao = S.Dt_aprovacao_producao
  , Comentarios_aprov_setor = S.Comentarios_aprov_setor
  , Status = S.Status
  , Nome_quem_aprovou_setor = S.Nome_quem_aprovou_setor
  , Nome_quem_aprovou_especialista = S.Nome_quem_aprovou_especialista
  , Modified = S.Modified
  , Status_aprov_setor = S.Status_aprov_setor
  , Status_aprov_especialista = S.Status_aprov_especialista
  , Codigos_produto = S.Codigos_produto
  , Dt_inicio_fluxo = S.Dt_inicio_fluxo
  , Status_aprov_analista = S.Status_aprov_analista
  , Nome_quem_aprovou_analista = S.Nome_quem_aprovou_analista
  , Dt_aprovacao_analista = S.Dt_aprovacao_analista
  , ID_anexo = S.ID_x0020_do_x0020_anexo
  , Nome_especialista = S.Nome_especialista
  , Nome_aprovador_setor = S.Nome_aprovador_setor
  , Email_analistas = S.Email_analistas
  , Email_sup_engenharia = S.Email_sup_engenharia
  , Dt_fap = S.Dt_fap
  , Solicitante = S.Solicitante
  , Tipo_melhoria = S.Tipo_melhoria
  , Setor = S.Setor
  , Numero_fap_pai = S.Numero_fap_pai
  , Guid_anexos = S.Guid_anexos
  , Cracha_idealizador = S.Cracha_idealizador
  , Nome_idealizador = S.Nome_idealizador
  , Cc_idealizador = S.Cc_idealizador
  , Setor_idealizador = S.Setor_idealizador
  , Gerente_idealizador = S.Gerente_idealizador
  , Cargo_idealizador = S.Cargo_idealizador
  , Tipo_fap = S.Tipo_fap
  , Dt_fap_informada_por = S.Dt_fap_informada_por
  , Desc_melhoria = S.Desc_melhoria
  , Investimento = S.Investimento
  , Custo_mod = S.Custo_mod
  , Desc_processo_atual = S.Desc_processo_atual
  , Desc_processo_proposto = S.Desc_processo_proposto
  , Carga_horaria_mes = S.Carga_horaria_mes
  , Gerente_melhoria = S.Gerente_melhoria
  , Created = S.Created
  , Comentarios_analista = S.Comentarios_analista
  , Certificacaoimetro = S.Certificacaoimetro
  , Status_aprovador_inmetro = S.Status_aprovador_inmetro
  , Dt_aprovacao_inmetro = S.Dt_aprovacao_inmetro
  , Comentario_aprovador_inmetro = S.Comentario_aprovador_inmetro
  , Nome_quem_aprovou_inmetro = S.Nome_quem_aprovou_inmetro
  , Eh_ganho = S.Eh_ganho
  , Quem_aprovou_gerencia_eng = S.Quem_aprovou_gerencia_eng
  , Status_aprovador_gerencia_eng = S.Status_aprovador_gerencia_eng
  , Comentarios_aprovador_gerencia_e = S.Comentarios_aprovador_gerencia_e
  , Dt_aprovacao_gerencia_eng = S.Dt_aprovacao_gerencia_eng
  , Tipo_produto = S.Tipo_produto
-- INSERT
WHEN NOT MATCHED BY TARGET
  THEN
  INSERT (
    Id
  , Fabrica_melhoria
  , Filial_melhoria
  , Origem_melhoria
  , Macro_setor_nome
  , Tipo_alteracao
  , Author
  , Editor
  , numero_fap
  , Email_aprovador_eng
  , Dt_aprovacao_eng
  , Comentarios_especialista
  , Email_aprovador_producao
  , Dt_aprovacao_producao
  , Comentarios_aprov_setor
  , Status
  , Nome_quem_aprovou_setor
  , Nome_quem_aprovou_especialista
  , Modified
  , Status_aprov_setor
  , Status_aprov_especialista
  , Codigos_produto
  , Dt_inicio_fluxo
  , Status_aprov_analista
  , Nome_quem_aprovou_analista
  , Dt_aprovacao_analista
  , ID_anexo
  , Nome_especialista
  , Nome_aprovador_setor
  , Email_analistas
  , Email_sup_engenharia
  , Dt_fap
  , Solicitante
  , Tipo_melhoria
  , Setor
  , Numero_fap_pai
  , Guid_anexos
  , Cracha_idealizador
  , Nome_idealizador
  , Cc_idealizador
  , Setor_idealizador
  , Gerente_idealizador
  , Cargo_idealizador
  , Tipo_fap
  , Dt_fap_informada_por
  , Desc_melhoria
  , Investimento
  , Custo_mod
  , Desc_processo_atual
  , Desc_processo_proposto
  , Carga_horaria_mes
  , Gerente_melhoria
  , Created
  , Comentarios_analista
  , Certificacaoimetro
  , Status_aprovador_inmetro
  , Dt_aprovacao_inmetro
  , Comentario_aprovador_inmetro
  , Nome_quem_aprovou_inmetro
  , Eh_ganho
  , Quem_aprovou_gerencia_eng
  , Status_aprovador_gerencia_eng
  , Comentarios_aprovador_gerencia_e
  , Dt_aprovacao_gerencia_eng
  , Tipo_produto
  )
  VALUES (
    S.Id
  , S.Fabrica_melhoria
  , S.Filial_melhoria
  , S.Origem_melhoria
  , S.Macro_setor
  , S.Tipo_alteracao
  , S.Author
  , S.Editor
  , S.Title
  , S.Email_aprovador_eng
  , S.Dt_aprovacao_eng
  , S.Comentarios_especialista
  , S.Email_aprovador_producao
  , S.Dt_aprovacao_producao
  , S.Comentarios_aprov_setor
  , S.Status
  , S.Nome_quem_aprovou_setor
  , S.Nome_quem_aprovou_especialista
  , S.Modified
  , S.Status_aprov_setor
  , S.Status_aprov_especialista
  , S.Codigos_produto
  , S.Dt_inicio_fluxo
  , S.Status_aprov_analista
  , S.Nome_quem_aprovou_analista
  , S.Dt_aprovacao_analista
  , S.[ID_x0020_do_x0020_anexo]
  , S.Nome_especialista
  , S.Nome_aprovador_setor
  , S.Email_analistas
  , S.Email_sup_engenharia
  , S.Dt_fap
  , S.Solicitante
  , S.Tipo_melhoria
  , S.Setor
  , S.Numero_fap_pai
  , S.Guid_anexos
  , S.Cracha_idealizador
  , S.Nome_idealizador
  , S.Cc_idealizador
  , S.Setor_idealizador
  , S.Gerente_idealizador
  , S.Cargo_idealizador
  , S.Tipo_fap
  , S.Dt_fap_informada_por
  , S.Desc_melhoria
  , S.Investimento
  , S.Custo_mod
  , S.Desc_processo_atual
  , S.Desc_processo_proposto
  , S.Carga_horaria_mes
  , S.Gerente_melhoria
  , S.Created
  , S.Comentarios_analista
  , S.Certificacaoimetro
  , S.Status_aprovador_inmetro
  , S.Dt_aprovacao_inmetro
  , S.Comentario_aprovador_inmetro
  , S.Nome_quem_aprovou_inmetro
  , S.Eh_ganho
  , S.Quem_aprovou_gerencia_eng
  , S.Status_aprovador_gerencia_eng
  , S.Comentarios_aprovador_gerencia_e
  , S.Dt_aprovacao_gerencia_eng
  , S.Tipo_produto
  )

-- DELETE
-- Deletamos no range de 90 dias.
WHEN NOT MATCHED BY SOURCE
AND T.Created >= DATEADD(DAY, -90, GETDATE())
  THEN DELETE;
