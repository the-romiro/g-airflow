DECLARE @sql NVARCHAR(MAX) = '';  -- Variável para armazenar a consulta final
DECLARE @tableName NVARCHAR(255); -- Variável para armazenar o nome da tabela
DECLARE @ID_Maq NVARCHAR(10);     -- Variável para armazenar o número extraído
DECLARE @filial NVARCHAR(2) ;

SET @filial = ?

-- Consultar os nomes das tabelas que começam com 'Ciclo'
DECLARE table_cursor CURSOR FOR
SELECT TABLE_NAME
FROM [Elipse].INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE 'Ciclo %'
AND TABLE_NAME NOT IN ('Ciclo 0', 'Ciclo_Plastisol')  -- Excluindo tabelas específicas
AND TABLE_TYPE = 'BASE TABLE';
 
-- Abrir o cursor
OPEN table_cursor;
 
-- Ler a primeira tabela
FETCH NEXT FROM table_cursor INTO @tableName;
 
-- Loop através das tabelas
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Extrair o número (ano) do nome da tabela, que vem após "Ciclo " (6 caracteres)
    SET @ID_Maq = SUBSTRING(@tableName, 7, LEN(@tableName) - 6);
 
    -- Criar a consulta SELECT para cada tabela, incluindo a coluna ID_Maq extraída
    IF LEN(@sql) > 0
    BEGIN
        SET @sql = @sql + ' UNION ALL ';
    END
 
    -- Adicionar o SELECT com a coluna ID_Maq extraída
    SET @sql = @sql +
            'SELECT '
            + @filial + ' AS id_estabelecimento,
            ''' + @ID_Maq + ''' AS id_equipamento,
            [E3TimeStamp]
			      ,[Ciclo Atual]
			      ,[Ciclo TI]
			      ,[Diferenca]
			      ,[Turno]
			      ,[Programa]
			      ,[Documento]
			      ,[NumeroProduto]
			      ,[Efetivo]
			      ,[ferramental]
			      ,[cod_refer_matriz]
			      ,[ParBat]
             FROM [Elipse].dbo.' + QUOTENAME(@tableName) + ' WITH(NOLOCK)'
                + ' WHERE E3TimeStamp >= ''{dt_inicio}'' AND E3TimeStamp < ''{dt_fim}'''
             ;
            -- E3TimeStamp >= DATEADD(DAY, -31, CONVERT(DATE, GETDATE()))

    -- Obter a próxima tabela
    FETCH NEXT FROM table_cursor INTO @tableName;
END
 
-- Fechar e desalocar o cursor
CLOSE table_cursor;
DEALLOCATE table_cursor;
 
-- Adicionar a criação da view à consulta final
SET @sql = @sql

-- Executar a consulta dinâmica
-- EXEC sp_executesql @sql;
SELECT @sql AS [SQL];