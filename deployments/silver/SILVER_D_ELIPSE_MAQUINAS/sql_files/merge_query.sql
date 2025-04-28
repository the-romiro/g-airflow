MERGE INTO elipse.silver.cadastros_maquinas maq
USING elipse.silver.temp_maquinas e
ON (maq.id = e."ID" and maq.id_estabelecimento = e.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        id_estabelecimento = e.id_estabelecimento,
        id = e."ID",
        id_setor = e."ID_Pavilhao",
        nome = e."Nome",
        descricao = e."Descricao",
				hora_criacao = e."Hora_Criacao",
        ip = e."IP",
				ativo = true,
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	id,
	    	id_setor,
	    	nome,
	    	descricao,
				hora_criacao,
	    	ip,
	    	ativo,
	    	data_atualizacao_db
	    	)
    VALUES (
	    	e.id_estabelecimento, 
	    	e."ID", 
	    	e."ID_Pavilhao",
	    	e."Nome",
	    	e."Descricao",
				e."Hora_Criacao",
	    	e."IP",
	    	true,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE AND data_desativacao is null THEN
    UPDATE SET 
		ativo = false,
		data_desativacao = COALESCE(data_desativacao, now() - INTERVAL '3 hours'),
    data_atualizacao_db = now() - INTERVAL '3 hours';


DROP TABLE elipse.silver.temp_maquinas;