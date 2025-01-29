MERGE INTO elipse.cadastros.maquinas maq
USING elipse.cadastros.temp_maquinas e
ON (maq.id = e."ID" and maq.id_estabelecimento = e.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        id_estabelecimento = e.id_estabelecimento,
        id = e."ID",
        id_pavilhao = e."ID_Pavilhao",
        nome = e."Nome",
        descricao = e."Descricao",
        ip = e."IP",
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	id,
	    	id_pavilhao,
	    	nome,
	    	descricao,
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
	    	e."IP",
	    	true,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
		ativo = false;


DROP TABLE elipse.cadastros.temp_maquinas;