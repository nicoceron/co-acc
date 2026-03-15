MATCH (p:Person)-[:RECEBEU_SALARIO]->(o:PublicOffice)
WITH p,
     count(DISTINCT o) AS office_count,
     collect(DISTINCT coalesce(o.role, o.cargo, o.name, o.org))[0..2] AS offices,
     coalesce(p.document_id, p.cedula, '') AS person_document_id
WHERE person_document_id <> ''
MATCH (c:Company {document_id: person_document_id})
MATCH ()-[award:CONTRATOU]->(c)
WITH p,
     office_count,
     offices,
     count(DISTINCT award.summary_id) AS supplier_contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_contract_value
ORDER BY supplier_contract_value DESC,
         supplier_contract_count DESC,
         coalesce(p.name, p.nome, p.full_name, p.document_id) ASC
LIMIT 120
CALL {
  WITH p
  OPTIONAL MATCH (p)-[d:DOOU]->(e:Election)
  RETURN count(DISTINCT e) AS donation_count,
         coalesce(sum(coalesce(d.value, 0.0)), 0.0) AS donation_value
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:CANDIDATO_EM]->(e:Election)
  RETURN count(DISTINCT e) AS candidacy_count
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU_BEM]->(a:DeclaredAsset)
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
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU_FINANCA]->(f:Finance)
  RETURN count(DISTINCT f) AS finance_count,
         coalesce(sum(coalesce(f.value, 0.0)), 0.0) AS finance_value
}
CALL {
  WITH p
  OPTIONAL MATCH (p)-[:DECLAROU_FINANCA]->(f:Finance {type: 'CONFLICT_DISCLOSURE'})
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
     coalesce(donation_count, 0) AS donation_count,
     coalesce(donation_value, 0.0) AS donation_value,
     coalesce(candidacy_count, 0) AS candidacy_count,
     coalesce(asset_count, 0) AS asset_count,
     coalesce(asset_value, 0.0) AS asset_value,
     coalesce(finance_count, 0) AS finance_count,
     coalesce(finance_value, 0.0) AS finance_value,
     coalesce(conflict_disclosure_count, 0) AS conflict_disclosure_count,
     coalesce(disclosure_reference_count, 0) AS disclosure_reference_count,
     coalesce(corporate_activity_disclosure_count, 0) AS corporate_activity_disclosure_count,
     CASE
       WHEN coalesce(donation_count, 0) > 0
       THEN supplier_contract_count
       ELSE 0
     END AS donor_vendor_loop_count
WITH p,
     offices,
     office_count,
     donation_count,
     donation_value,
     candidacy_count,
     asset_count,
     asset_value,
     finance_count,
     finance_value,
     supplier_contract_count,
     supplier_contract_value,
     conflict_disclosure_count,
     disclosure_reference_count,
     corporate_activity_disclosure_count,
     donor_vendor_loop_count,
     (
       2 +
       CASE WHEN donation_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN candidacy_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN conflict_disclosure_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN disclosure_reference_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN corporate_activity_disclosure_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types,
     (
       5 +
       CASE
         WHEN supplier_contract_count >= 10 THEN 4
         WHEN supplier_contract_count >= 3 THEN 3
         ELSE 2
       END +
       CASE
         WHEN supplier_contract_value >= 10000000000 THEN 3
         WHEN supplier_contract_value >= 1000000000 THEN 2
         WHEN supplier_contract_value >= 100000000 THEN 1
         ELSE 0
       END +
       CASE
         WHEN donation_count > 0 THEN 3
         ELSE 0
       END +
       CASE
         WHEN candidacy_count > 0 THEN 2
         ELSE 0
       END +
       CASE
         WHEN conflict_disclosure_count > 0 THEN 3
         ELSE 0
       END +
       CASE
         WHEN disclosure_reference_count >= 3 THEN 3
         WHEN disclosure_reference_count >= 1 THEN 2
         ELSE 0
       END +
       CASE
         WHEN corporate_activity_disclosure_count > 0 THEN 2
         ELSE 0
       END
     ) AS suspicion_score
RETURN elementId(p) AS entity_id,
       coalesce(p.name, p.nome, p.full_name, p.document_id) AS name,
       p.document_id AS document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(office_count) AS office_count,
       toInteger(donation_count) AS donation_count,
       toFloat(donation_value) AS donation_value,
       toInteger(candidacy_count) AS candidacy_count,
       toInteger(asset_count) AS asset_count,
       toFloat(asset_value) AS asset_value,
       toInteger(finance_count) AS finance_count,
       toFloat(finance_value) AS finance_value,
       toInteger(supplier_contract_count) AS supplier_contract_count,
       toFloat(supplier_contract_value) AS supplier_contract_value,
       toInteger(conflict_disclosure_count) AS conflict_disclosure_count,
       toInteger(disclosure_reference_count) AS disclosure_reference_count,
       toInteger(corporate_activity_disclosure_count) AS corporate_activity_disclosure_count,
       toInteger(donor_vendor_loop_count) AS donor_vendor_loop_count,
       offices AS offices
ORDER BY suspicion_score DESC,
         donor_vendor_loop_count DESC,
         disclosure_reference_count DESC,
         supplier_contract_value DESC,
         donation_value DESC,
         name ASC
LIMIT toInteger($limit)
