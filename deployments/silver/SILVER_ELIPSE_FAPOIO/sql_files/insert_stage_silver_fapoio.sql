delete from elipse.silver.elipse_fapoio
WHERE 
	data_hora_inicio >= CURRENT_DATE - INTERVAL '60 days' 
	or data_hora_fim >= CURRENT_DATE - INTERVAL '60 days';
	
  -- data_hora_inicio between '2025-02-01 05:25:00' and '2025-03-01 06:00:00'
  -- or data_hora_fim between '2025-02-01 05:25:00' and '2025-03-01 06:00:00'
  -- or (data_hora_inicio < '2025-02-01 05:25:00' and data_hora_fim > '2025-03-01 06:00:00');

insert into elipse.silver.elipse_fapoio(
	id_estabelecimento,
	id,
	data_hora_inicio,
	data_hora_fim,
	codigo,
	maquina_id,
	status,
	tempo,
	cracha_preparador,
	cracha_lider,
	data_atualizacao_db
)
select 
		id_estabelecimento,
		"Id",
		"DataHoraInicio",
		"DataHoraFim",
		"CodigoMotivo",
		"Maquina_ID",
		"Status",
		"Tempo",
		"Cracha_Preparador",
		"Cracha_Lider",
		now() - INTERVAL '3 hours'	
from elipse.stage.stage_apoio;