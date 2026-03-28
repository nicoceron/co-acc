MATCH (c:Company)
WHERE elementId(c) = $company_id
   OR c.document_id = $company_identifier
   OR c.document_id = $company_identifier_formatted
   OR c.nit = $company_identifier
   OR c.nit = $company_identifier_formatted
CALL {
  WITH c
  OPTIONAL MATCH (:Company)-[ia:CELEBRO_CONVENIO_INTERADMIN]->(c)
  RETURN count(DISTINCT ia.summary_id) AS interadmin_agreement_count,
         coalesce(sum(coalesce(ia.total_value, 0.0)), 0.0) AS interadmin_total,
         collect(DISTINCT ia.summary_id) AS interadmin_ids,
         min(coalesce(ia.first_date, ia.last_date)) AS interadmin_window_start,
         max(coalesce(ia.last_date, ia.first_date)) AS interadmin_window_end
}
CALL {
  WITH c
  OPTIONAL MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_total,
         count(
           DISTINCT CASE
             WHEN coalesce(award.invoice_total_value, 0.0) > 0.0
              AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
             THEN award.summary_id
             ELSE NULL
           END
         ) AS execution_gap_contract_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.commitment_total_value, 0.0) > 0.0
              AND coalesce(award.invoice_total_value, 0.0) >
                  coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
             THEN award.summary_id
             ELSE NULL
           END
         ) AS commitment_gap_contract_count,
         count(
           DISTINCT CASE
             WHEN coalesce(award.suspension_event_count, 0) > 0
             THEN award.summary_id
             ELSE NULL
           END
         ) AS suspension_contract_count,
         collect(DISTINCT award.summary_id) AS award_ids,
         min(coalesce(award.first_date, award.last_date)) AS contract_window_start,
         max(coalesce(award.last_date, award.first_date)) AS contract_window_end
}
CALL {
  WITH c
  OPTIONAL MATCH (p:Person)-[:OFFICER_OF]->(c)
  WHERE EXISTS { MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice) }
  RETURN count(DISTINCT p) AS official_officer_count
}
CALL {
  WITH c
  OPTIONAL MATCH (c)-[:SANCIONADA]->(s:Sanction)
  RETURN count(DISTINCT s) AS sanction_count
}
WITH c,
     interadmin_agreement_count,
     interadmin_total,
     contract_count,
     contract_total,
     execution_gap_contract_count,
     commitment_gap_contract_count,
     suspension_contract_count,
     official_officer_count,
     sanction_count,
     CASE
       WHEN interadmin_window_start IS NOT NULL
        AND (contract_window_start IS NULL OR interadmin_window_start < contract_window_start)
       THEN interadmin_window_start
       ELSE contract_window_start
     END AS window_start,
     CASE
       WHEN interadmin_window_end IS NOT NULL
        AND (contract_window_end IS NULL OR interadmin_window_end > contract_window_end)
       THEN interadmin_window_end
       ELSE contract_window_end
     END AS window_end,
     [x IN interadmin_ids + award_ids WHERE x IS NOT NULL AND x <> ''] AS evidence_refs,
     (
       CASE WHEN official_officer_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN sanction_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN execution_gap_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN commitment_gap_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN suspension_contract_count > 0 THEN 1 ELSE 0 END
     ) AS stack_signal_types
WHERE interadmin_agreement_count > 0
  AND contract_count > 0
  AND stack_signal_types > 0
RETURN 'interadministrative_channel_stacking' AS pattern_id,
       coalesce(c.document_id, c.nit) AS company_identifier,
       coalesce(c.razon_social, c.name) AS company_name,
       toFloat(
         interadmin_agreement_count +
         contract_count +
         stack_signal_types +
         official_officer_count +
         sanction_count
       ) AS risk_signal,
       toInteger(interadmin_agreement_count) AS interadmin_agreement_count,
       toFloat(interadmin_total) AS interadmin_total,
       toInteger(contract_count) AS contract_count,
       toFloat(contract_total) AS contract_total,
       toInteger(official_officer_count) AS official_officer_count,
       toInteger(sanction_count) AS sanction_count,
       toInteger(execution_gap_contract_count) AS execution_gap_contract_count,
       toInteger(commitment_gap_contract_count) AS commitment_gap_contract_count,
       toInteger(suspension_contract_count) AS suspension_contract_count,
       toInteger(stack_signal_types) AS stack_signal_types,
       interadmin_total + contract_total AS amount_total,
       window_start AS window_start,
       window_end AS window_end,
       evidence_refs[0..toInteger($pattern_max_evidence_refs)] AS evidence_refs,
       size(evidence_refs) AS evidence_count
