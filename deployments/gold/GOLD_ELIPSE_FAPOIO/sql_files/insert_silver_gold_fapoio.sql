DELETE FROM elipse.gold.elipse_fapoio
WHERE
	data_hora_inicio >= CURRENT_DATE - INTERVAL '90 days'
	or data_hora_fim >= CURRENT_DATE - INTERVAL '90 days';


INSERT INTO elipse.gold.elipse_fapoio(
  id_estabelecimento,
  id,
  data_hora_inicio,
  data_hora_fim,
  tempo,
  status,
  codigo,
  descricao_motivo,
  familia,
  peso_oee,
  maquina_id,
  nome_equipamento,
  id_setor,
  setor,
  id_fabrica,
  fabrica,
  cracha_preparador,
  cracha_lider
)
select
	apoio.id_estabelecimento,
	apoio.id,
	apoio.data_hora_inicio,
	apoio.data_hora_fim,
	apoio.tempo,
	apoio.status,
	apoio.codigo,
	mtv.descricao        as descricao_motivo,
	cfa.descricao        as familia,
	mtv.peso_oee,
	apoio.maquina_id,
	maq.nome             as nome_equipamento,
	maq.id_setor,
	cs.descricao         as setor,
	cs.id_fabrica,
	cf.descricao         as fabrica,
	apoio.cracha_preparador,
	apoio.cracha_lider
from elipse.silver.elipse_fapoio apoio
left join elipse.silver.cadastros_motivos mtv on apoio.codigo = mtv.codigo and apoio.id_estabelecimento = mtv.id_estabelecimento
left join elipse.silver.cadastros_maquinas maq on apoio.maquina_id = maq.id and apoio.id_estabelecimento = maq.id_estabelecimento
left join elipse.silver.cadastros_setores cs on maq.id_setor = cs.id and maq.id_estabelecimento = cs.id_estabelecimento
left join elipse.silver.cadastros_fabricas cf on cs.id_fabrica = cf.id and cs.id_estabelecimento = cf.id_estabelecimento
left join elipse.silver.cadastros_familias cfa on mtv.id_familia = cfa.id and mtv.id_estabelecimento = cfa.id_estabelecimento
WHERE
  data_hora_inicio >= CURRENT_DATE - INTERVAL '90 days'
	or data_hora_fim >= CURRENT_DATE - INTERVAL '90 days'
ON CONFLICT DO NOTHING;
-- ON CONFLICT (id_estabelecimento, id)
-- DO UPDATE SET
--   data_hora_inicio     = EXCLUDED.data_hora_inicio,
--   data_hora_fim        = EXCLUDED.data_hora_fim,
--   tempo                = EXCLUDED.tempo,
--   status               = EXCLUDED.status,
--   codigo               = EXCLUDED.codigo,
--   descricao_motivo     = EXCLUDED.descricao_motivo,
--   familia              = EXCLUDED.familia,
--   peso_oee             = EXCLUDED.peso_oee,
--   maquina_id           = EXCLUDED.maquina_id,
--   nome_equipamento     = EXCLUDED.nome_equipamento,
--   id_setor             = EXCLUDED.id_setor,
--   setor                = EXCLUDED.setor,
--   id_fabrica           = EXCLUDED.id_fabrica,
--   fabrica              = EXCLUDED.fabrica,
--   cracha_preparador    = EXCLUDED.cracha_preparador,
--   cracha_lider         = EXCLUDED.cracha_lider;
