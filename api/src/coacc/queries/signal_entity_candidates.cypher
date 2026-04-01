CALL {
  MATCH (c:Company)
  WHERE coalesce(c.nit, c.document_id, '') <> ''
  RETURN elementId(c) AS entity_id,
         coalesce(c.nit, c.document_id) AS entity_key,
         'Company' AS entity_label
  UNION
  MATCH (p:Person)
  WHERE coalesce(p.document_key, p.document_id, p.cedula, '') <> ''
  RETURN elementId(p) AS entity_id,
         coalesce(p.document_key, p.document_id, p.cedula) AS entity_key,
         'Person' AS entity_label
  UNION
  MATCH (pr:Project)
  WHERE coalesce(pr.project_id, pr.bpin_code, '') <> ''
  RETURN elementId(pr) AS entity_id,
         coalesce(pr.project_id, pr.bpin_code) AS entity_key,
         'Project' AS entity_label
  UNION
  MATCH (ct:Contract)
  WHERE coalesce(ct.contract_id, ct.summary_id, '') <> ''
  RETURN elementId(ct) AS entity_id,
         coalesce(ct.contract_id, ct.summary_id) AS entity_key,
         'Contract' AS entity_label
}
RETURN entity_id, entity_key, entity_label
ORDER BY entity_label, entity_key
LIMIT coalesce($limit, 1000)
