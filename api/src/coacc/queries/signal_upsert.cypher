MERGE (hit:SignalHit {hit_id: $hit_id})
ON CREATE SET
  hit.created_at = datetime($created_at),
  hit.first_seen_at = datetime($first_seen_at)
SET
  hit.run_id = $run_id,
  hit.signal_id = $signal_id,
  hit.signal_version = $signal_version,
  hit.title = $title,
  hit.description = $description,
  hit.category = $category,
  hit.severity = $severity,
  hit.public_safe = $public_safe,
  hit.reviewer_only = $reviewer_only,
  hit.entity_id = $entity_id,
  hit.entity_key = $entity_key,
  hit.entity_label = $entity_label,
  hit.scope_key = $scope_key,
  hit.scope_type = $scope_type,
  hit.dedup_key = $dedup_key,
  hit.score = $score,
  hit.identity_confidence = $identity_confidence,
  hit.identity_match_type = $identity_match_type,
  hit.identity_quality = $identity_quality,
  hit.evidence_count = $evidence_count,
  hit.evidence_refs = $evidence_refs,
  hit.data_json = $data_json,
  hit.sources = $sources,
  hit.last_seen_at = datetime($last_seen_at)
WITH hit
MATCH (run:SignalRun {run_id: $run_id})
MERGE (run)-[:MATERIALIZO]->(hit)
MERGE (definition:SignalDefinition {signal_id: $signal_id})
SET definition.version = $signal_version,
    definition.title = $title,
    definition.description = $description,
    definition.category = $category,
    definition.severity = $severity,
    definition.public_safe = $public_safe,
    definition.reviewer_only = $reviewer_only,
    definition.updated_at = datetime($last_seen_at)
MERGE (hit)-[:DEFINIDO_POR]->(definition)
MERGE (run)-[:EJECUTO_DEFINICION]->(definition)
WITH hit
OPTIONAL MATCH (entity)
WHERE (entity.document_id = $entity_id OR entity.document_id = $entity_key
   OR entity.document_key = $entity_id OR entity.document_key = $entity_key
   OR entity.nit = $entity_id OR entity.nit = $entity_key
   OR entity.cedula = $entity_id OR entity.cedula = $entity_key
   OR entity.numero_documento = $entity_id OR entity.numero_documento = $entity_key
   OR entity.contract_id = $entity_id OR entity.contract_id = $entity_key
   OR entity.sanction_id = $entity_id OR entity.sanction_id = $entity_key
   OR entity.amendment_id = $entity_id OR entity.amendment_id = $entity_key
   OR entity.finance_id = $entity_id OR entity.finance_id = $entity_key
   OR entity.embargo_id = $entity_id OR entity.embargo_id = $entity_key
   OR entity.school_id = $entity_id OR entity.school_id = $entity_key
   OR entity.convenio_id = $entity_id OR entity.convenio_id = $entity_key
   OR entity.partner_id = $entity_id OR entity.partner_id = $entity_key
   OR entity.bid_id = $entity_id OR entity.bid_id = $entity_key
   OR entity.asset_id = $entity_id OR entity.asset_id = $entity_key
   OR entity.office_id = $entity_id OR entity.office_id = $entity_key
   OR entity.stats_id = $entity_id OR entity.stats_id = $entity_key
   OR entity.project_id = $entity_id OR entity.project_id = $entity_key
   OR entity.bpin_code = $entity_id OR entity.bpin_code = $entity_key
   OR entity.case_id = $entity_id OR entity.case_id = $entity_key
   OR entity.act_id = $entity_id OR entity.act_id = $entity_key
   OR entity.gaceta_id = $entity_id OR entity.gaceta_id = $entity_key
   OR entity.requirement_id = $entity_id OR entity.requirement_id = $entity_key
   OR entity.session_id = $entity_id OR entity.session_id = $entity_key
   OR entity.order_id = $entity_id OR entity.order_id = $entity_key
   OR entity.file_id = $entity_id OR entity.file_id = $entity_key
   OR entity.doc_id = $entity_id OR entity.doc_id = $entity_key
   OR elementId(entity) = $entity_id)
  AND (entity:Person OR entity:Partner OR entity:Company OR entity:Contract OR entity:Sanction
       OR entity:Election OR entity:Amendment OR entity:Finance OR entity:Embargo
       OR entity:Health OR entity:Education OR entity:Convenio OR entity:LaborStats
       OR entity:PublicOffice OR entity:Bid OR entity:DeclaredAsset OR entity:Inquiry
       OR entity:SourceDocument OR entity:Project OR entity:JudicialCase
       OR entity:ActoAdministrativo OR entity:GacetaTerritorial
       OR entity:InquiryRequirement OR entity:InquirySession
       OR entity:TVECOrder OR entity:EnvironmentalFile)
FOREACH (_ IN CASE WHEN entity IS NULL THEN [] ELSE [1] END |
  MERGE (hit)-[:PARA]->(entity)
)
MERGE (bundle:EvidenceBundle {bundle_id: $bundle_id})
ON CREATE SET
  bundle.created_at = datetime($created_at)
SET
  bundle.signal_id = $signal_id,
  bundle.headline = $bundle_headline,
  bundle.summary = $bundle_summary,
  bundle.entity_key = $entity_key,
  bundle.scope_key = $scope_key,
  bundle.scope_type = $scope_type,
  bundle.source_list = $sources,
  bundle.updated_at = datetime($last_seen_at)
MERGE (hit)-[:SUSTENTADO_POR]->(bundle)
WITH bundle
OPTIONAL MATCH (bundle)-[:CONTIENE]->(existing:EvidenceItem)
WHERE NOT existing.item_id IN [entry IN $evidence_items | entry.item_id]
DETACH DELETE existing
WITH bundle
UNWIND $evidence_items AS item
MERGE (evidence:EvidenceItem {item_id: item.item_id})
SET
  evidence.source_id = item.source_id,
  evidence.record_id = item.record_id,
  evidence.url = item.url,
  evidence.label = item.label,
  evidence.item_type = item.item_type,
  evidence.node_ref = item.node_ref,
  evidence.observed_at = CASE
    WHEN item.observed_at IS NULL THEN null
    ELSE datetime(item.observed_at)
  END,
  evidence.public_safe = item.public_safe,
  evidence.identity_match_type = item.identity_match_type,
  evidence.identity_quality = item.identity_quality
MERGE (bundle)-[:CONTIENE]->(evidence)
