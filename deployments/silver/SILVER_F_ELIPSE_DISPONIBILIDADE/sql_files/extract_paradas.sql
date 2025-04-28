SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as linha,
    ? AS id_estabelecimento,
    Id,
    Maquina_ID,
    Codigo,
    Hora_Inicio,
    E3TimeStamp,
    tempo,
    Turno,
    Status,
    Programa,
    Documento,
    NumeroProduto,
    Efetivo,
    ID_Grupo,
    Ref_Matriz,
    Ferramental,
    Cracha_Operador,
    Cracha_Preparador,
    Cracha_Lider
FROM [Elipse].[dbo].[Paradas] WITH(NOLOCK)
WHERE 
(Hora_Inicio >= DATEADD(DAY, -60, CONVERT(DATE, GETDATE())))
OR 
(E3TimeStamp >= DATEADD(DAY, -60, CONVERT(DATE, GETDATE())))
