MERGE INTO elipse.silver.cadastros_motivos m
USING elipse.silver.temp_motivos e
ON (m.codigo = e."Codigo" and m.id_estabelecimento = e.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        id_estabelecimento = e.id_estabelecimento,
        codigo = e."Codigo",
        descricao = e."Descricao",
        peso_oee = e."PesoOEE",
        id_familia = e."ID_Familia",
        data_criacao = e."E3TimeStamp",
        ativo = true,
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	codigo,
	    	descricao,
	    	peso_oee,
	    	id_familia,
	    	data_criacao,
        ativo,
	    	data_atualizacao_db
	    	)
    VALUES (
	    	e.id_estabelecimento, 
	    	e."Codigo", 
	    	e."Descricao",
	    	e."PesoOEE",
	    	e."ID_Familia",
	    	e."E3TimeStamp",
        true,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
		ativo = false,
    data_atualizacao_db = now() - INTERVAL '3 hours';


DROP TABLE elipse.silver.temp_motivos;