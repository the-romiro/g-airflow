SELECT CAST('{id_equipamento}' AS INT) AS id_equipamento,
  MAX(E3TimeStamp) AS max_ts
FROM [Elipse].dbo.[{table_name}] WITH(NOLOCK)
