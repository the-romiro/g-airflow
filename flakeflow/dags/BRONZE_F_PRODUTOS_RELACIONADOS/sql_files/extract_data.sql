--Tabela ProdutosRelacionados
SELECT
    l.id as log_id,
    sku->>'estab' as estabelecimento,
    sku->>'produto' as produto,
    sku->>'codProduto' as codProduto,
    sku->>'descProduto' as descProduto,
    sku->>'desMarcaGrendene' as desMarcaGrendene,
    sku->>'ultimoLancamento' as ultimoLancamento,
    sku->>'descricaoComponente' as descricaoComponente
FROM workflow_logs l
CROSS JOIN LATERAL jsonb_array_elements(l.data->'corpo'->'produtosRelacionados') sku
WHERE workflow_id = '8b301608-9bc5-4042-9835-7e02d4beec52'
AND tipo = 'WEB_HOOK'
AND l.data->'corpo' ? 'produtosRelacionados'
