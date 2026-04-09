SELECT
    {estab} AS id_estabelecimento,
    N'{id_equipamento}' AS id_equipamento,
    [E3TimeStamp],
    [Ciclo Atual],
    [Ciclo TI],
    [Diferenca],
    [Turno],
    [Programa],
    [Documento],
    [NumeroProduto],
    [Efetivo],
    [ferramental],
    [cod_refer_matriz],
    [ParBat]
FROM [Elipse].dbo.[{table_name}] WITH(NOLOCK)
WHERE E3TimeStamp >= '{dt_inicio}' AND E3TimeStamp < '{dt_fim}'
