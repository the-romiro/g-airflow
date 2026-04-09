--Tabela Area.

SELECT
  wl.id,
  wl.criado_em as Data_Criacao,
  wl.atualizado_em as atualizadoem,
  wl.status as status,
  param->>'id_flake' AS id_flake,
  param->>'cod_usuario' AS cod_usuario,
  ferr->>'desArea' AS des_area,
  TO_DATE(ferr->>'datReserva','DD/MM/YYYY') AS data_reserva,
  ferr->>'desJustificativa' AS justificativa,
  sku->>'codSku' AS cod_sku,
  (regexp_matches(
      input->>'code',
      'getWorkflowVariable\("([a-f0-9\-]+)\.numeroProcesso"\)'
  ))[1] AS webhook_node_id
FROM workflow_logs wl
LEFT JOIN LATERAL jsonb_array_elements(
  wl.data->'result'->'corpoParaAPIReservar'->'param'
) param ON TRUE
LEFT JOIN LATERAL jsonb_array_elements(
  wl.data->'result'->'corpoParaAPIReservar'->'Ferramentais'
) ferr ON TRUE
LEFT JOIN LATERAL jsonb_array_elements(
  ferr->'skuFerramentas'
) sku ON TRUE
WHERE wl.workflow_id = '8b301608-9bc5-4042-9835-7e02d4beec52'
AND wl.tipo = 'CODE' and wl.data->'result'->>'corpoParaAPIReservar' is not null
