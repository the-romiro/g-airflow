SELECT id_equipamento, MAX(data_hora) AS ultima_data_hora
FROM silver.oee_fciclos
WHERE id_estabelecimento = {estab_code}
GROUP BY id_equipamento
