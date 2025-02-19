SELECT
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
    Ferramental
FROM [Elipse].[dbo].[Paradas] WITH(NOLOCK)
WHERE 
(Hora_Inicio >= DATEADD(DAY, -31, CONVERT(DATE, GETDATE())))
OR 
(E3TimeStamp >= DATEADD(DAY, -31, CONVERT(DATE, GETDATE())))
