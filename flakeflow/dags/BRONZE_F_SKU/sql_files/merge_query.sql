INSERT INTO ferramental.sku (log_id, sku, descricao, status, qtdcif, fabrica, ferramenta, qtd_estoque)
SELECT
    log_id::uuid,
    sku,
    descricao,
    status,
    qtdcif,
    fabrica,
    ferramenta,
    qtd_estoque
FROM stage.stage_sku
ON CONFLICT (log_id) DO NOTHING
