--Tabela SKU.
SELECT
    l.id as log_id,
    sku->>'sku' as sku,
    sku->>'descricao' as descricao,
    sku->>'status' as status,
    (sku->>'qtdCif')::float4 as qtdCif,
    sku->>'codFabrica' as Fabrica,
    sku->>'ferramenta' as ferramenta,
    (sku->>'qtdEstoque')::float4 as qtd_estoque
FROM workflow_logs l
CROSS JOIN LATERAL jsonb_array_elements(l.data->'corpo'->'skuFerramentas') sku
WHERE workflow_id = '8b301608-9bc5-4042-9835-7e02d4beec52'
AND tipo = 'WEB_HOOK'
AND l.data->'corpo' ? 'skuFerramentas'
