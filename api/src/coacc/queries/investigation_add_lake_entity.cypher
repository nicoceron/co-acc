MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation {id: $investigation_id})
MERGE (e:LakeEntityRef {entity_uid: $entity_uid})
SET e.document_id = $document_id,
    e.nit = $nit,
    e.cedula = $cedula,
    e.name = $name,
    e.razon_social = CASE WHEN $entity_type = "company" THEN $name ELSE e.razon_social END,
    e.entity_type = $entity_type,
    e.entity_label = $entity_label,
    e.source = $source_list,
    e.identity_quality = $identity_quality,
    e.exposure_tier = $exposure_tier,
    e.updated_at = datetime()
FOREACH (_ IN CASE WHEN $entity_type = "company" THEN [1] ELSE [] END | SET e:Company)
FOREACH (_ IN CASE WHEN $entity_type = "person" THEN [1] ELSE [] END | SET e:Person)
MERGE (i)-[:INCLUDES]->(e)
SET i.updated_at = datetime(),
    i.case_scope_updated_at = datetime(),
    i.case_stale = true
RETURN i.id AS investigation_id,
       coalesce(e.document_id, e.nit, e.cedula, e.entity_uid) AS entity_id
