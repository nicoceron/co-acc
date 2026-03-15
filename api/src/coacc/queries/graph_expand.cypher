MATCH (center)
WHERE elementId(center) = $entity_id
  AND (
    center:Person OR center:Company OR center:Contract OR center:Sanction OR center:Election
    OR center:Amendment OR center:Finance OR center:Embargo OR center:Health OR center:Education
    OR center:Convenio OR center:LaborStats OR center:PublicOffice OR center:Bid
    OR center:DeclaredAsset OR center:Inquiry
    OR center:OffshoreEntity OR center:OffshoreOfficer OR center:GlobalPEP
    OR center:CVMProceeding OR center:Expense
  )
CALL apoc.path.expandConfig(
  center,
  {
    relationshipFilter: "SOCIO_DE|DOOU|CANDIDATO_EM|VENCEU|CONTRATOU|ADJUDICOU_A|AUTOR_EMENDA|SANCIONADA|OPERA_UNIDADE|DEVE|RECEBEU_EMPRESTIMO|EMBARGADA|MANTEDORA_DE|BENEFICIOU|GEROU_CONVENIO|SAME_AS|POSSIBLY_SAME_AS|OFFICER_OF|INTERMEDIARY_OF|GLOBAL_PEP_MATCH|CVM_SANCIONADA|GASTOU|FORNECEU|FORNECEU_LICITACAO|LICITOU|DECLAROU_BEM|DECLAROU_FINANCA|RECEBEU_SALARIO|REFERENTE_A|ADMINISTRA",
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
