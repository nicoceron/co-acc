MATCH (center)
WHERE (
    elementId(center) = $entity_id
    OR replace(coalesce(center.document_id, ''), '-', '') = replace(trim(coalesce($entity_id, '')), '-', '')
    OR replace(coalesce(center.nit, ''), '-', '') = replace(trim(coalesce($entity_id, '')), '-', '')
    OR coalesce(center.bid_id, '') = trim(coalesce($entity_id, ''))
    OR coalesce(center.doc_id, '') = trim(coalesce($entity_id, ''))
  )
  AND (
    center:Person OR center:Company OR center:Contract OR center:Sanction OR center:Election
    OR center:Amendment OR center:Finance OR center:Embargo OR center:Health OR center:Education
    OR center:Convenio OR center:LaborStats OR center:PublicOffice OR center:Bid
    OR center:DeclaredAsset OR center:Inquiry OR center:Finding OR center:SourceDocument
    OR center:OffshoreEntity OR center:OffshoreOfficer OR center:GlobalPEP
    OR center:CVMProceeding OR center:Expense
  )
CALL apoc.path.expandConfig(
  center,
  {
    relationshipFilter: "SOCIO_DE|DONO_A|CANDIDATO_EM|GANO|CONTRATOU|ADJUDICOU_A|AUTOR_EMENDA|SANCIONADA|TIENE_HALLAZGO|OPERA_UNIDAD|DEVE|RECEBEU_EMPRESTIMO|EMBARGADA|MANTIENE_A|BENEFICIO|GENERO_CONVENIO|CELEBRO_CONVENIO_INTERADMIN|SAME_AS|POSSIBLY_SAME_AS|POSSIBLE_FAMILY_TIE|OFFICER_OF|SUPERVISA_PAGO|INTERMEDIARY_OF|GLOBAL_PEP_MATCH|CVM_SANCIONADA|GASTO|SUMINISTRO|SUMINISTRO_LICITACAO|LICITO|DECLARO_BIEN|DECLARO_FINANZAS|RECIBIO_SALARIO|REFERENTE_A|ADMINISTRA",
    labelFilter: $label_filter,
    maxLevel: toInteger($depth),
    bfs: true,
    uniqueness: "RELATIONSHIP_PATH",
    optional: true,
    limit: toInteger($path_limit)
  }
) YIELD path
WITH center, collect(path) AS paths
WITH center,
     reduce(ns = [center], p IN paths | ns + CASE WHEN p IS NULL THEN [] ELSE nodes(p) END) AS raw_nodes,
     reduce(rs = [], p IN paths | rs + CASE WHEN p IS NULL THEN [] ELSE relationships(p) END) AS raw_rels
UNWIND raw_nodes AS n
WITH center, collect(DISTINCT n) AS nodes, raw_rels
UNWIND CASE WHEN size(raw_rels) = 0 THEN [NULL] ELSE raw_rels END AS r
WITH center, nodes, collect(DISTINCT r) AS rels
RETURN nodes,
       [x IN rels WHERE x IS NOT NULL] AS relationships,
       elementId(center) AS center_id
