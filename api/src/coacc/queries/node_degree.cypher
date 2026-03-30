MATCH (n)
WHERE (
    elementId(n) = $entity_id
    OR replace(coalesce(n.document_id, ''), '-', '') = replace(trim(coalesce($entity_id, '')), '-', '')
    OR replace(coalesce(n.nit, ''), '-', '') = replace(trim(coalesce($entity_id, '')), '-', '')
    OR coalesce(n.bid_id, '') = trim(coalesce($entity_id, ''))
    OR coalesce(n.doc_id, '') = trim(coalesce($entity_id, ''))
  )
  AND (n:Person OR n:Company OR n:Contract OR n:Sanction OR n:Election
       OR n:Amendment OR n:Finance OR n:Embargo OR n:Health OR n:Education
       OR n:Convenio OR n:LaborStats OR n:PublicOffice OR n:Bid OR n:DeclaredAsset
       OR n:Inquiry OR n:Finding OR n:SourceDocument)
RETURN COUNT { (n)--() } AS degree
