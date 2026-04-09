--Tabela Eventos.
select
  --tipo,
  --data->>'corpo' as Corpo,
  id as ID_Log,
  criado_em as Data_Criacao,
  atualizado_em as Atualizado_Em,
  data->'corpo'->>'estab' as Estabelecimento,
  data->'corpo'->>'mercado' as Mercado,
  data->'corpo'->>'produto' as Produto,
  (data->'corpo'->>'codEstab')::int2 as CodEstabelecimento,
  data->'corpo'->>'deposito' as Deposito,
  data->'corpo'->>'codDeposito'as codDeposito,
  data->'corpo'->>'codProduto' as codProduto,
  data->'corpo'->>'descProduto' as descProduto,
  (data->'corpo'->>'qtdeEliminar')::int4 as qtdeEliminar,
  data->'corpo'->>'marcaGrendene' as marcaGrendene,
  data->'corpo'->>'statusProduto' as statusProduto,
  data->'corpo'->>'valorResidual' as valorResidual,
  data->'corpo'->>'descFerramenta' as descFerramenta,
  data->'corpo'->>'numeroProcesso' as numeroProcesso,
  data->'corpo'->>'tipoFerramenta' as tipoFerramenta,
  (data->'corpo'->>'qtdeEstoqueAtual')::float4 as qtdeEstoqueAtual,
  to_date(data->'corpo'->>'dataLimiteAnalise','DD/MM/YYYY') as dataLimiteAnalise,
  (data->'corpo'->>'segmentoDeNegocio')::int4 as segmentoDeNegocio,
  data->'corpo'->>'codGrupoFerramental' as codGrupoFerramental,
  data->'corpo'->>'descSegmentoNegocio' as descSegmentoNegocio,
  data->'corpo'->>'descGrupoFerramental' as descGrupoFerramental
FROM workflow_logs
where workflow_id = '8b301608-9bc5-4042-9835-7e02d4beec52' and data is not null and tipo = 'WEB_HOOK' and data->'corpo'->>'estab' is not null

