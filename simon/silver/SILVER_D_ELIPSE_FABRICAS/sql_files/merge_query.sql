MERGE INTO elipse.silver.cadastros_fabricas fab
USING elipse.silver.temp_fabricas temp
ON (fab.id_estabelecimento = temp.id_estabelecimento and fab.id = temp."ID")
WHEN MATCHED THEN 
  UPDATE SET
    id_estabelecimento = temp.id_estabelecimento,
    id = temp."ID",
    descricao = temp."Fabrica",
    ativo = true,
    data_atualizacao_db = NOW() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
   INSERT(
    id_estabelecimento,
    id,
    descricao,
    ativo,
    data_atualizacao_db
   )
   VALUES(
    temp.id_estabelecimento,
    temp."ID",
    temp."Fabrica",
    true,
    NOW() - INTERVAL '3 hours'
   )
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET
    ativo = false,
    data_atualizacao_db = NOW() - INTERVAL '3 hours';

DROP TABLE elipse.silver.temp_fabricas;