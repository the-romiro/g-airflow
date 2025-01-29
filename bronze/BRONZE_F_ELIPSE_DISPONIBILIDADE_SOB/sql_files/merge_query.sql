MERGE INTO elipse.oee.paradas p
USING elipse.oee.temp_paradas o
ON (p.id = o."Id" and p.id_estabelecimento = o.id_estabelecimento)
WHEN MATCHED THEN
	UPDATE SET
        maquina_id = o."Maquina_ID",
        codigo = o."Codigo",
        data_hora_inicio = o."Hora_Inicio",
        data_hora_fim = CASE WHEN o."Status" = 1 then null else o."E3TimeStamp" END,
        tempo = o.tempo,
        turno = o."Turno",
        status = o."Status",
        programa = o."Programa",
        documento = o."Documento",
        numero_produto = o."NumeroProduto",
        efetivo = o."Efetivo",
        id_grupo = o."ID_Grupo",
        ref_matriz = o."Ref_Matriz",
        ferramental = o."Ferramental",
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
	    	id,
	    	maquina_id, 
	    	codigo,
	    	data_hora_inicio,
	    	data_hora_fim,
	    	tempo,
	    	turno,
	    	status,
	    	programa,
	    	documento,
	    	numero_produto,
	    	efetivo,
	    	id_grupo,
	    	ref_matriz,
	    	ferramental,
	    	data_atualizacao_db
	    	)
    VALUES (
	    	o.id_estabelecimento, 
	    	o."Id", 
	    	o."Maquina_ID",
	    	o."Codigo",
	    	o."Hora_Inicio",
	    	CASE WHEN o."Status" = 1 then null else o."E3TimeStamp" END,
	    	o.tempo,
	    	o."Turno",
	    	o."Status",
	    	o."Programa",
	    	o."Documento",
	    	o."NumeroProduto",
	    	o."Efetivo",
	    	o."ID_Grupo",
	    	o."Ref_Matriz",
	    	o."Ferramental",
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE AND (p.data_hora_inicio >= CURRENT_DATE - INTERVAL '10 days' or p.data_hora_fim >= CURRENT_DATE - INTERVAL '10 days') THEN
    DELETE;


DROP TABLE elipse.oee.temp_paradas;