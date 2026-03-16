MATCH (p:Person)
WHERE exists(p.document_id) OR exists(p.cedula)
WITH p,
     coalesce(p.document_id, p.cedula, '') AS person_document_id

// Signal 1: Public Offices (SIGEP)
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN count(DISTINCT o) AS office_count,
         collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..2] AS offices
}

// Signal 2: Supplier Contracts (SECOP)
CALL {
  WITH p, person_document_id
  OPTIONAL MATCH (c:Company {document_id: person_document_id})
  OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS supplier_contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_contract_value
}

// Signal 3: Sanctions (PACO/SECOP)
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:SANCIONADA]->(s:Sanction)
  RETURN count(DISTINCT s) AS sanction_count
}

// Signal 4: Asset Disclosures (Ley 2013)
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU]->(a:DeclaredAsset)
  RETURN count(DISTINCT a) AS asset_count,
         coalesce(sum(coalesce(a.value, 0.0)), 0.0) AS asset_value,
         count(
           DISTINCT CASE
             WHEN coalesce(a.has_board_roles, false)
               OR coalesce(a.has_corporate_interests, false)
               OR coalesce(a.has_private_activities, false)
             THEN a
             ELSE null
           END
         ) AS corporate_activity_disclosure_count
}

// Signal 5: Conflict Disclosures
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
  RETURN count(DISTINCT f) AS conflict_disclosure_count,
         sum(
           coalesce(f.company_document_mention_count, 0)
           + coalesce(f.company_name_mention_count, 0)
           + coalesce(f.process_reference_count, 0)
         ) AS disclosure_reference_count
}

WITH p,
     offices,
     office_count,
     supplier_contract_count,
     supplier_contract_value,
     sanction_count,
     asset_count,
     asset_value,
     conflict_disclosure_count,
     disclosure_reference_count,
     corporate_activity_disclosure_count,
     0 AS donation_count,
     0.0 AS donation_value,
     0 AS candidacy_count,
     0 AS donor_vendor_loop_count

// Filter: Only return people with at least 2 distinct signals (multi-signal)
WHERE (
  CASE WHEN office_count > 0 THEN 1 ELSE 0 END +
  CASE WHEN supplier_contract_count > 0 THEN 1 ELSE 0 END +
  CASE WHEN sanction_count > 0 THEN 1 ELSE 0 END +
  CASE WHEN asset_count > 0 THEN 1 ELSE 0 END +
  CASE WHEN conflict_disclosure_count > 0 THEN 1 ELSE 0 END +
  CASE WHEN corporate_activity_disclosure_count > 0 THEN 1 ELSE 0 END
) >= 2

WITH p,
     offices,
     office_count,
     donation_count,
     donation_value,
     candidacy_count,
     asset_count,
     asset_value,
     supplier_contract_count,
     supplier_contract_value,
     conflict_disclosure_count,
     disclosure_reference_count,
     corporate_activity_disclosure_count,
     donor_vendor_loop_count,
     (
       CASE WHEN office_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN supplier_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN sanction_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN asset_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN conflict_disclosure_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN disclosure_reference_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN corporate_activity_disclosure_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types,
     (
       CASE
         WHEN sanction_count > 0 THEN 40
         ELSE 0
       END +
       CASE
         WHEN office_count > 0 THEN 20
         ELSE 0
       END +
       CASE
         WHEN supplier_contract_count >= 5 THEN 30
         WHEN supplier_contract_count >= 1 THEN 15
         ELSE 0
       END +
       CASE
         WHEN asset_count >= 10 THEN 20
         WHEN asset_count >= 1 THEN 10
         ELSE 0
       END +
       CASE
         WHEN conflict_disclosure_count > 0 THEN 25
         ELSE 0
       END +
       CASE
         WHEN corporate_activity_disclosure_count > 0 THEN 20
         ELSE 0
       END
     ) AS suspicion_score

RETURN elementId(p) AS entity_id,
       coalesce(p.name, p.nombre, p.full_name, p.document_id) AS name,
       p.document_id AS document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(office_count) AS office_count,
       toInteger(donation_count) AS donation_count,
       toFloat(donation_value) AS donation_value,
       toInteger(candidacy_count) AS candidacy_count,
       toInteger(asset_count) AS asset_count,
       toFloat(asset_value) AS asset_value,
       toInteger(0) AS finance_count,
       toFloat(0.0) AS finance_value,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       toFloat(supplier_contract_value) AS supplier_contract_value,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(disclosure_reference_count) AS disclosure_reference_count,
       toInteger(corporate_activity_disclosure_count) AS corporate_activity_disclosure_count,
       toInteger(donor_vendor_loop_count) AS donor_vendor_loop_count,
       offices AS offices
ORDER BY suspicion_score DESC,
         disclosure_reference_count DESC,
         supplier_contract_value DESC,
         name ASC
LIMIT toInteger($limit)
