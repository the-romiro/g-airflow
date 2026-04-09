MERGE INTO elipse.silver.cadastros_setores s
USING elipse.silver.temp_setores e
ON (s.id = e."ID" and s.id_estabelecimento = e.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        id_estabelecimento = e.id_estabelecimento,
        id = e."ID",
        descricao = e."Pavilhao",
        id_fabrica = e."ID_Fabrica",
        ativo = true,
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	id,
        descricao,
        id_fabrica,
        ativo,
	    	data_atualizacao_db
	    	)
    VALUES (
	    	e.id_estabelecimento, 
	    	e."ID", 
	    	e."Pavilhao",
	    	e."ID_Fabrica",
        true,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
		ativo = false,
    data_atualizacao_db = now() - INTERVAL '3 hours';


DROP TABLE elipse.silver.temp_setores;