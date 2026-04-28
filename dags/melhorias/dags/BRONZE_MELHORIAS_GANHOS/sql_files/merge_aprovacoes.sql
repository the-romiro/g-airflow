DECLARE @FIRST_MODIFIED_DATE AS DATETIME
SELECT @FIRST_MODIFIED_DATE = MIN(Modified) FROM Dbo.Stg_mel_aprovacao

MERGE Dbo.Mel_aprovacao AS Target
USING Dbo.Stg_mel_aprovacao AS Source
ON Target.Id = Source.Id

-- UPDATE (incremental)
WHEN MATCHED
AND (
  Target.Modified <> Source.Modified
  OR Target.Modified IS NULL AND Source.Modified IS NOT NULL
  OR Target.Modified IS NOT NULL AND Source.Modified IS NULL
)
  THEN UPDATE SET
    Fabrica_melhoria = Source.Fabrica_melhoria
  , Filial_melhoria = Source.Filial_melhoria
  , Origem_melhoria = Source.Origem_melhoria
  , Macro_setor_nome = Source.Macro_setor
  , Tipo_alteracao = Source.Tipo_alteracao
  , Author = Source.Author
  , Editor = Source.Editor
  , Title = Source.Title
  , Email_aprovador_eng = Source.Email_aprovador_eng
  , Dt_aprovacao_eng = Source.Dt_aprovacao_eng
  , Comentarios_especialista = Source.Comentarios_especialista
  , Email_aprovador_producao = Source.Email_aprovador_producao
  , Dt_aprovacao_producao = Source.Dt_aprovacao_producao
  , Comentarios_aprov_setor = Source.Comentarios_aprov_setor
  , Status = Source.Status
  , Nome_quem_aprovou_setor = Source.Nome_quem_aprovou_setor
  , Nome_quem_aprovou_especialista = Source.Nome_quem_aprovou_especialista
  , Modified = Source.Modified
  , Status_aprov_setor = Source.Status_aprov_setor
  , Status_aprov_especialista = Source.Status_aprov_especialista
  , Codigos_produto = Source.Codigos_produto
  , Dt_inicio_fluxo = Source.Dt_inicio_fluxo
  , Status_aprov_analista = Source.Status_aprov_analista
  , Nome_quem_aprovou_analista = Source.Nome_quem_aprovou_analista
  , Dt_aprovacao_analista = Source.Dt_aprovacao_analista
  , ID_x0020_do_x0020_anexo = Source.ID_x0020_do_x0020_anexo
  , Nome_especialista = Source.Nome_especialista
  , Nome_aprovador_setor = Source.Nome_aprovador_setor
  , Email_analistas = Source.Email_analistas
  , Email_sup_engenharia = Source.Email_sup_engenharia
  , Dt_fap = Source.Dt_fap
  , Solicitante = Source.Solicitante
  , Tipo_melhoria = Source.Tipo_melhoria
  , Setor = Source.Setor
  , Numero_fap_pai = Source.Numero_fap_pai
  , Guid_anexos = Source.Guid_anexos
  , Cracha_idealizador = Source.Cracha_idealizador
  , Nome_idealizador = Source.Nome_idealizador
  , Cc_idealizador = Source.Cc_idealizador
  , Setor_idealizador = Source.Setor_idealizador
  , Gerente_idealizador = Source.Gerente_idealizador
  , Cargo_idealizador = Source.Cargo_idealizador
  , Tipo_fap = Source.Tipo_fap
  , Dt_fap_informada_por = Source.Dt_fap_informada_por
  , Desc_melhoria = Source.Desc_melhoria
  , Investimento = Source.Investimento
  , Custo_mod = Source.Custo_mod
  , Desc_processo_atual = Source.Desc_processo_atual
  , Desc_processo_proposto = Source.Desc_processo_proposto
  , Carga_horaria_mes = Source.Carga_horaria_mes
  , Gerente_melhoria = Source.Gerente_melhoria
  , Created = Source.Created
  , Comentarios_analista = Source.Comentarios_analista
  , Certificacaoimetro = Source.Certificacaoimetro
  , Status_aprovador_inmetro = Source.Status_aprovador_inmetro
  , Dt_aprovacao_inmetro = Source.Dt_aprovacao_inmetro
  , Comentario_aprovador_inmetro = Source.Comentario_aprovador_inmetro
  , Nome_quem_aprovou_inmetro = Source.Nome_quem_aprovou_inmetro
  , Eh_ganho = Source.Eh_ganho
  , Quem_aprovou_gerencia_eng = Source.Quem_aprovou_gerencia_eng
  , Status_aprovador_gerencia_eng = Source.Status_aprovador_gerencia_eng
  , Comentarios_aprovador_gerencia_e = Source.Comentarios_aprovador_gerencia_e
  , Dt_aprovacao_gerencia_eng = Source.Dt_aprovacao_gerencia_eng
  , Tipo_produto = Source.Tipo_produto

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
  , Title
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
  , Id_x0020_do_x0020_anexo
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
    Source.Id
  , Source.Fabrica_melhoria
  , Source.Filial_melhoria
  , Source.Origem_melhoria
  , Source.Macro_setor
  , Source.Tipo_alteracao
  , Source.Author
  , Source.Editor
  , Source.Title
  , Source.Email_aprovador_eng
  , Source.Dt_aprovacao_eng
  , Source.Comentarios_especialista
  , Source.Email_aprovador_producao
  , Source.Dt_aprovacao_producao
  , Source.Comentarios_aprov_setor
  , Source.Status
  , Source.Nome_quem_aprovou_setor
  , Source.Nome_quem_aprovou_especialista
  , Source.Modified
  , Source.Status_aprov_setor
  , Source.Status_aprov_especialista
  , Source.Codigos_produto
  , Source.Dt_inicio_fluxo
  , Source.Status_aprov_analista
  , Source.Nome_quem_aprovou_analista
  , Source.Dt_aprovacao_analista
  , Source.Id_x0020_do_x0020_anexo
  , Source.Nome_especialista
  , Source.Nome_aprovador_setor
  , Source.Email_analistas
  , Source.Email_sup_engenharia
  , Source.Dt_fap
  , Source.Solicitante
  , Source.Tipo_melhoria
  , Source.Setor
  , Source.Numero_fap_pai
  , Source.Guid_anexos
  , Source.Cracha_idealizador
  , Source.Nome_idealizador
  , Source.Cc_idealizador
  , Source.Setor_idealizador
  , Source.Gerente_idealizador
  , Source.Cargo_idealizador
  , Source.Tipo_fap
  , Source.Dt_fap_informada_por
  , Source.Desc_melhoria
  , Source.Investimento
  , Source.Custo_mod
  , Source.Desc_processo_atual
  , Source.Desc_processo_proposto
  , Source.Carga_horaria_mes
  , Source.Gerente_melhoria
  , Source.Created
  , Source.Comentarios_analista
  , Source.Certificacaoimetro
  , Source.Status_aprovador_inmetro
  , Source.Dt_aprovacao_inmetro
  , Source.Comentario_aprovador_inmetro
  , Source.Nome_quem_aprovou_inmetro
  , Source.Eh_ganho
  , Source.Quem_aprovou_gerencia_eng
  , Source.Status_aprovador_gerencia_eng
  , Source.Comentarios_aprovador_gerencia_e
  , Source.Dt_aprovacao_gerencia_eng
  , Source.Tipo_produto
  )

-- DELETE
WHEN NOT MATCHED BY SOURCE
AND Target.Modified >= @FIRST_MODIFIED_DATE
  THEN DELETE;
