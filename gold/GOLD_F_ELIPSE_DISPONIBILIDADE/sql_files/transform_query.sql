with 
cte_identificacao_setup_estratificado as (
	select distinct
		p.id_grupo,
		max(tp.tipo_setup) as tipo_setup
	from elipse.silver.oee_fparadas p
	left join elipse.silver.cadastros_identificador_setup tp
	on p.codigo = tp.codigo and p.id_estabelecimento = tp.id_estabelecimento
	where id_grupo > 0
	group by p.id_grupo
),
cte_paradas as
(
select
	p.id_estabelecimento,
	p.id as id_parada,
	p.data_hora_inicio,
	p.data_hora_fim,
	p.tempo,
	p.codigo,
	cm.descricao,
	cfa.descricao as familia,
	cfaa.descricao as familia_agrupada,
	case 
		when cm.descricao like '%Aguardando%' and cfaa.descricao = 'Matrizaria' then 'Aguardando Matrizaria'
		when cm.descricao not like '%Aguardando%' and cfaa.descricao = 'Matrizaria' then 'Reparo Matrizaria' 
		when cm.descricao like '%Aguardando%' and cfaa.descricao = 'Manutenção' then 'Aguardando Manutenção'
		when cm.descricao not like '%Aguardando%' and cfaa.descricao = 'Manutenção' then 'Reparo Manutenção' 
	end as tipo,
	p.status,
	p.turno,
	cm.peso_oee,
	p.id_grupo,
	case
		when ids.tipo_setup is null then
			case 
				when p.id_estabelecimento = 20 then
					case  p.codigo
						when 100 then 'Setup de Cor'
						when 107 then 'Setup de Matriz'
						when 125 then 'Troca de Layout'
						when 126 then 'Troca de Numeração'
						when 149 then 'Troca de Produto'
						when 402 then 'Troca de Coleção'
						when 816 then 'Deslocamento de Equipe'
						when 826 then 'Ligada de Máquina'
						when 892 then 'Setup de Cor - Limpeza de Bico'
						when 1701 then 'Setup de Cor - Glitter'
					end
				when p.id_estabelecimento = 21 then
					case  p.codigo
						when 100 then 'Setup de Cor'
						when 107 then 'Setup de Matriz'
						when 126 then 'Troca de Numeração'
						when 149 then 'Troca de Produto'
						when 184 then 'Setup de Cor - Glitter'
						when 422 then 'Deslocamento de Equipe'	
					end
				when p.id_estabelecimento = 40 then
					case  p.codigo
						when 100 then 'Setup de Cor'
						when 107 then 'Setup de Matriz'
						when 125 then 'Troca de Layout'
						when 126 then 'Troca de Numeração'
						when 149 then 'Troca de Produto'
						when 150 then 'Troca de Grade'
						when 402 then 'Troca de Coleção'
						when 422 then 'Deslocamento de Equipe'	
					end		
			end 
		else ids.tipo_setup
	end as tipo_setup,
	maq.id as id_equipamento,
	maq.nome as nome_equipamento,
	cf.descricao as fabrica,
	cs.descricao  as setor,
	p.ferramental,
	p.ref_matriz
from elipse.silver.oee_fparadas p
inner join elipse.silver.cadastros_maquinas maq
on p.maquina_id  = maq.id and p.id_estabelecimento = maq.id_estabelecimento
inner join elipse.silver.cadastros_setores cs
on cs.id = maq.id_setor and p.id_estabelecimento = cs.id_estabelecimento
inner join elipse.silver.cadastros_fabricas cf
on cs.id_fabrica = cf.id and p.id_estabelecimento = cf.id_estabelecimento
inner join elipse.silver.cadastros_motivos cm
on p.codigo = cm.codigo and p.id_estabelecimento = cm.id_estabelecimento
inner join elipse.silver.cadastros_familias cfa
on cm.id_familia = cfa.id and cfa.id_estabelecimento = cm.id_estabelecimento
left join elipse.silver.cadastros_familias_agrupadas cfaa
on cfa.id_familia_agrupada = cfaa.id 
left join cte_identificacao_setup_estratificado ids
on ids.id_grupo = p.id_grupo
where p.data_hora_inicio >= CURRENT_DATE - INTERVAL '31 days'
or p.data_hora_fim >= CURRENT_DATE - INTERVAL '31 days'
or p.data_hora_fim is null),
cte_paradas_final as (
select 
	id_estabelecimento,
	id_parada,
	data_hora_inicio,
	coalesce(data_hora_fim, now() - INTERVAL '3 hours') as data_hora_fim,
	tempo,
	codigo,
	descricao,
	familia,
	case 
		when tipo_setup is not null then 'Setup' else familia_agrupada
	end as familia_agrupada,
	tipo,
	status,
	turno,
	peso_oee,
	case 
		when id_grupo is null and tipo_setup is not null then left(md5(id_parada::TEXT || id_equipamento::TEXT), 8) else id_grupo::text
	end as id_grupo,
	tipo_setup,
	id_equipamento,
	nome_equipamento,
	fabrica,
	setor,
	ferramental,
	ref_matriz
from cte_paradas
)
MERGE INTO elipse.gold.oee_fparadas p
USING cte_paradas_final o
ON (p.id_parada = o.id_parada and p.id_estabelecimento = o.id_estabelecimento)
WHEN MATCHED AND (
    p.data_hora_inicio <> o.data_hora_inicio OR
    p.data_hora_fim <> o.data_hora_fim OR
    p.tempo <> o.tempo OR
    p.codigo <> o.codigo OR
    p.descricao <> o.descricao OR
    p.familia <> o.familia OR
    p.familia_agrupada <> o.familia_agrupada OR
    p.tipo <> o.tipo OR
    p.status <> o.status OR
    p.turno <> o.turno OR
    p.peso_oee <> o.peso_oee OR
    p.id_grupo <> o.id_grupo OR
    p.tipo_setup <> o.tipo_setup OR
    p.id_equipamento <> o.id_equipamento OR
    p.nome_equipamento <> o.nome_equipamento OR
    p.fabrica <> o.fabrica OR
    p.setor <> o.setor OR
    p.ferramental <> o.ferramental OR
    p.ref_matriz <> o.ref_matriz
)THEN
	UPDATE SET
        id_estabelecimento = o.id_estabelecimento,
        id_parada = o.id_parada,
        data_hora_inicio = o.data_hora_inicio,
        data_hora_fim = o.data_hora_fim,
	      tempo = o.tempo,
	      codigo = o.codigo,
	      descricao = o.descricao,
	      familia = o.familia,
        familia_agrupada = o.familia_agrupada,
        tipo = o.tipo,
	      status = o.status,
	      turno = o.turno,
	      peso_oee = o.peso_oee,
        id_grupo = o.id_grupo,
        tipo_setup = o.tipo_setup,
	      id_equipamento = o.id_equipamento,
	      nome_equipamento = o.nome_equipamento,
	      fabrica = o.fabrica,
	      setor = o.setor,
	      ferramental = o.ferramental,
	      ref_matriz = o.ref_matriz,
        data_atualizacao_db = now() - INTERVAL '3 hours'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
	    	id_estabelecimento,
        id_parada,
        data_hora_inicio,
        data_hora_fim,
	      tempo,
	      codigo,
	      descricao,
	      familia,
        familia_agrupada ,
        tipo,
	      status,
	      turno,
	      peso_oee,
        id_grupo,
        tipo_setup,
	      id_equipamento,
	      nome_equipamento,
	      fabrica,
	      setor,
	      ferramental,
	      ref_matriz,
        data_atualizacao_db
	    	)
    VALUES (
	    	o.id_estabelecimento,
        o.id_parada,
        o.data_hora_inicio,
        o.data_hora_fim,
	      o.tempo,
	      o.codigo,
	      o.descricao,
	      o.familia,
        o.familia_agrupada,
        o.tipo,
	      o.status,
	      o.turno,
	      o.peso_oee,
        o.id_grupo,
        o.tipo_setup,
	      o.id_equipamento,
	      o.nome_equipamento,
	      o.fabrica,
	      o.setor,
	      o.ferramental,
	      o.ref_matriz,
	    	now() - INTERVAL '3 hours'
    	)
WHEN NOT MATCHED BY SOURCE AND (p.data_hora_inicio >= CURRENT_DATE - INTERVAL '31 days' or p.data_hora_fim >= CURRENT_DATE - INTERVAL '31 days') THEN
    DELETE;