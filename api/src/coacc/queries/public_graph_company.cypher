MATCH (center:Company)
WHERE elementId(center) = $company_id
   OR center.document_id = $company_identifier
   OR center.document_id = $company_identifier_formatted
   OR center.nit = $company_identifier
   OR center.nit = $company_identifier_formatted
   OR center.cnpj = $company_identifier
   OR center.cnpj = $company_identifier_formatted
CALL apoc.path.expandConfig(
  center,
  {
    relationshipFilter: "SOCIO_DE|GANO|CONTRATOU|ADJUDICOU_A|SANCIONADA|DEVE|RECEBEU_EMPRESTIMO|BENEFICIO|GENERO_CONVENIO|MUNICIPAL_GANO|MUNICIPAL_LICITO|LICITO|SUMINISTRO_LICITACAO|REFERENTE_A|ADMINISTRA",
    labelFilter: "+Company|+Contract|+Sanction|+Finance|+Amendment|+Convenio|+Bid|+MunicipalContract|+MunicipalBid|-Person|-Partner|-User|-Investigation|-Annotation|-Tag",
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
