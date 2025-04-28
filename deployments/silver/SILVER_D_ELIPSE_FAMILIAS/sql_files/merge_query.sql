MERGE INTO elipse.silver.cadastros_familias f
USING elipse.silver.temp_familias e
ON (f.id = e."ID" and f.id_estabelecimento = e.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        id_estabelecimento = e.id_estabelecimento,
        id = e."ID",
        indice = e."Indice",
        descricao = e."Familia",
        data_criacao = e."E3TimeStamp",
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	id,
        indice,
        descricao,
	    	data_criacao,
        ativo,
	    	data_atualizacao_db
	    	)
    VALUES (
	    	e.id_estabelecimento, 
	    	e."ID", 
	    	e."Indice",
	    	e."Familia",
	    	e."E3TimeStamp",
        true,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
		ativo = false,
    data_atualizacao_db = now() - INTERVAL '3 hours';


DROP TABLE elipse.silver.temp_familias;