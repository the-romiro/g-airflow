MERGE INTO elipse.silver.cadastros_produtos as cp
USING elipse.silver.temp_produtos as o ON cp.id_estabelecimento = o.id_estabelecimento and cp.id = o."ID"
when matched and (
	cp.id_estabelecimento is distinct from o.id_estabelecimento or
	cp.id is distinct from o."ID" or
	cp.id_equipamento is distinct from o."ID_Maq" or
	cp.numero_produto is distinct from o."NumeroProduto" or
	cp.programa is distinct from o."Programa" or
	cp.documento is distinct from o."Documento" or
	cp.data_inicio is distinct from o."DataInicio" or
	cp.data_fim is distinct from o."DataFim" or
	cp.ciclo is distinct from o."Ciclo" or
	cp.pares_batidas is distinct from o."ParesBatidas" or
	cp.efetivo is distinct from o."Efetivo"
)then
	update set
		id_estabelecimento = o.id_estabelecimento,
		id = o."ID",
		id_equipamento = o."ID_Maq",
		numero_produto = o."NumeroProduto",
		programa = o."Programa",
		documento = o."Documento",
		data_inicio = o."DataInicio",
		data_fim = o."DataFim",
		ciclo = o."Ciclo",
		pares_batidas = o."ParesBatidas",
		efetivo = o."Efetivo",
		data_atualizacao_db = now() - INTERVAL '3 hours'
when not matched by target then
	insert(
		id_estabelecimento,
		id,
		id_equipamento,
		numero_produto,
		programa,
		documento,
		data_inicio,
		data_fim,
		ciclo,
		pares_batidas,
		efetivo,
		data_atualizacao_db
	)
	values(
		o.id_estabelecimento,
		o."ID",
		o."ID_Maq",
		o."NumeroProduto",
		o."Programa",
		o."Documento",
		o."DataInicio",
		o."DataFim",
		o."Ciclo",
		o."ParesBatidas",
		o."Efetivo",
		now() - INTERVAL '3 hours'
	)
when not matched by source and (cp.data_inicio >= CURRENT_DATE - INTERVAL '31 days' or cp.data_fim >= CURRENT_DATE - INTERVAL '31 days') then
	delete;


drop table elipse.silver.temp_produtos


