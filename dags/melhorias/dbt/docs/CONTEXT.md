# CONTEXT — Melhorias (migração Qlik → SQL + dbt)

Glossário do domínio de Melhorias (Indicador de Melhorias / Aderência / Ganhos).
Apenas termos de negócio. Decisões de implementação vão em `docs/adr/`.

## Termos

### Melhoria
Proposta de melhoria de processo registrada pela engenharia. Identificada por um
**Nº F.A.P**. Tem um **Idealizador**, um **Tipo de Melhoria** e gera **Ganho**.

### FAP (Nº F.A.P)
Ficha de Alteração de Processo. Identificador de negócio de uma Melhoria.
Uma FAP passa por um fluxo de **Aprovação**.

### FAO (Nº F.A.P)
Ficha de Aproveitamento de Obsoleto. Identificador de negócio de uma Melhoria de aproveitamento de material obsoleto (parado mais de 30 dias em estoque).
Uma FAO passa por um fluxo de **Aprovação**.

### Aprovação / Assinatura
Fluxo de aprovação de uma Melhoria (especialista, setor, analista, gerência eng,
inmetro). Origem: lista SharePoint `Coletar_Assinaturas` → tabela `mel_aprovacao`.
`Status = 'Aprovado'` + `dt_aprovacao_eng` definem quando a melhoria conta.

### Ganho
Resultado financeiro/produtivo de uma Melhoria. Três tipos mutuamente exclusivos:
- **Tempo Ciclo (TC)** — horas ganhas → pessoas → R$ via custo do funcionário.
- **Aglutinação** — Mesmo que **TC**.
- **Consumo** — economia de matéria-prima (Cons_Anterior - Cons_Atual).
- **Troca de Matéria Prima (Troca MP)** — diferença de custo par.
- **Outros tipos** — O ganho fica em `Ganho mês R$` e `mel_ganhos.ganho_previsto`
Origem: lista SharePoint `Melhorias` → tabela `mel_ganhos`.

### Meta
Alvo de melhorias por **Gerente** e **Bimestre**. Define quem é **Elegível**
(`Grupo Cargo` <> 'Não Elegível' E `Categoria Situação` = 'Considera').
Origem: planilha de metas → tabela `mel_meta_aderencia`.

### Aderência
Indicador que compara o **Realizado** (melhorias por idealizador/gerente) contra a
**Meta** do bimestre. É o KPI central do dashboard. Exemplo: meta 1 fez 1 melhoria, meta ok, meta 1 fez 'n' melhorias, meta ok.

### Em andamento
Melhoria ainda não aprovada (`dt_fap` null). **Não** entra nos fatos de aprovadas
(`fct_ganhos`, `fct_aderencia`); vive num fato próprio `fct_melhorias_em_andamento`
(pipeline), referenciada por `Criado` em vez de `dt_fap`. Carrega `status` do fluxo
e `ganho_previsto`. Decisão consciente: KPI principal só conta aprovadas.

### Data da FAP (dt_fap)
**Data mestra** da Melhoria — quando a melhoria "conta". Deriva de
`dt_aprovacao_eng` (`status='Aprovado'`). É o único link temporal dos fatos com
`dim_calendario` (ganhos e aderência). Unifica o que o Qlik espalhava em "Data da
FAP" (ganhos) e "Data para Meta" (aderência).

### Idealizador
Colaborador que registra a Melhoria. Identidade = **(Bimestre, Crachá)**, não só
crachá: gerente, cargo, grupo de cargo e elegibilidade variam por bimestre. Um
idealizador por bimestre. Chave equivalente ao `%ChaveCodigoBimestre` do Qlik.

### Bimestre
Grão temporal central. 6 bimestres/ano (jan-fev, mar-abr, …, nov-dez).
Identificado como `<ano>_Bimestre_<n>`. Derivado da data (não há mais "bimestre
corrente" pinado por config — gold reconstrói todo histórico; Power BI fatia).

### Gerente
Responsável por uma área. Sofre **remapeamento** (Qlik tinha cadeia de if's:
Andre Miorelli→Andre - F4, etc.) e join com tabela de áreas (`gerentes`).

### Elegível
Flag derivada na Meta: colaborador conta para aderência quando `Grupo Cargo` é
elegível e `Categoria Situação` = 'Considera'.

## Camadas (medallion → SQL Server dbengenharia, schema dbo)

- **Bronze**: ingestão crua do SharePoint (`mel_aprovacao`, `mel_ganhos`). Source
  imutável; dbt não escreve aqui. Ingerida pelas DAGs Airflow.
- **Silver**: limpeza/derivações como VIEWS dbt (ex.: derivar `dt_fap` em vez de
  UPDATE in-place).
- **Gold**: 3 fatos (`fct_ganhos`, `fct_aderencia`, `fct_meta`) + dims conformes
  (`dim_calendario`, `dim_gerente`, `dim_idealizador`, `dim_tipo_melhoria`), tudo
  como VIEWS, consumido pelo Power BI em modo import. Ver `adr/0001`.

## Tabelas auxiliares

Duas origens, conforme a natureza do dado:

- **Seeds dbt** (CSV versionado no repo, estático/quase-estático; tabelas com
  prefixo `mel_`): `mel_gerente_remap` + `mel_gerente_area` (remap + área),
  `mel_categoria_melhorias` (tipo→categoria), `mel_bimestre` (mês→bimestre).
  dbt cria a tabela via `dbt seed`.
- **Sources populadas via planilha + VBA** (direto no SQL Server, sem DAG):
  `mel_meta_aderencia` (por bimestre), `mel_carga_horaria` (mensal),
  `mel_custo_funcionario` (mensal). O negócio mantém via planilha/VBA; dbt só as
  declara como `sources` e lê.

A primeira entrega (decisão: tudo de uma vez — 3 fatos + dims) **depende** das
aux populadas; o `dbt build` só fecha com seeds carregadas e sources ingeridas.
