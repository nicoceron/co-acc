MATCH ()-[award:CONTRATOU]->(c:Company)
WITH trim(coalesce(award.buyer_document_id, '')) AS buyer_document_id,
     trim(coalesce(award.buyer_name, '')) AS buyer_name,
     c,
     toInteger(sum(coalesce(award.contract_count, 1))) AS supplier_contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_value,
     toInteger(sum(
       CASE
         WHEN coalesce(award.offer_count, 0) <= 2 OR coalesce(award.direct_invitation, false)
         THEN coalesce(award.contract_count, 1)
         ELSE 0
       END
     )) AS supplier_low_competition_contract_count,
     toInteger(sum(
       CASE
         WHEN coalesce(award.direct_invitation, false)
         THEN coalesce(award.contract_count, 1)
         ELSE 0
       END
     )) AS supplier_direct_invitation_contract_count,
     toInteger(sum(
       CASE
         WHEN coalesce(award.invoice_total_value, 0.0) > 0.0
           AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
         THEN coalesce(award.contract_count, 1)
         ELSE 0
       END
     )) AS supplier_execution_gap_contract_count,
     toInteger(sum(
       CASE
         WHEN coalesce(award.commitment_total_value, 0.0) > 0.0
           AND coalesce(award.invoice_total_value, 0.0) >
               coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
         THEN coalesce(award.contract_count, 1)
         ELSE 0
       END
     )) AS supplier_commitment_gap_contract_count,
     toInteger(sum(
       CASE
         WHEN (
           coalesce(award.invoice_total_value, 0.0) > 0.0
           AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
         ) OR (
           coalesce(award.commitment_total_value, 0.0) > 0.0
           AND coalesce(award.invoice_total_value, 0.0) >
               coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
         )
         THEN coalesce(award.contract_count, 1)
         ELSE 0
       END
     )) AS supplier_discrepancy_contract_count,
     coalesce(
       sum(
         CASE
           WHEN coalesce(award.invoice_total_value, 0.0) > 0.0
             AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
           THEN coalesce(award.invoice_total_value, 0.0)
           WHEN coalesce(award.commitment_total_value, 0.0) > 0.0
             AND coalesce(award.invoice_total_value, 0.0) >
                 coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
           THEN coalesce(award.invoice_total_value, 0.0) - coalesce(award.commitment_total_value, 0.0)
           ELSE 0.0
         END
       ),
       0.0
     ) AS supplier_discrepancy_value,
     EXISTS {
       MATCH (c)-[:SANCIONADA]->(:Sanction)
     } AS supplier_has_sanction
CALL {
  WITH c
  OPTIONAL MATCH (p:Person)-[:OFFICER_OF]->(c)
  WHERE EXISTS {
    MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice)
  }
  RETURN count(DISTINCT p) AS supplier_official_overlap_count
}
CALL {
  WITH c, supplier_value
  OPTIONAL MATCH (c)-[:DECLARO_FINANZAS]->(f:Finance {type: 'SUPERSOC_TOP_COMPANY'})
  RETURN max(
           CASE
             WHEN f IS NOT NULL AND (
               (
                 coalesce(f.operating_revenue_current, 0.0) > 0.0
                 AND supplier_value / toFloat(f.operating_revenue_current) >= 2.0
               ) OR (
                 coalesce(f.total_assets_current, 0.0) > 0.0
                 AND supplier_value / toFloat(f.total_assets_current) >= 1.0
               )
             )
             THEN 1
             ELSE 0
           END
         ) AS supplier_capacity_mismatch_flag
}
WITH *
WHERE buyer_document_id <> '' OR buyer_name <> ''
WITH CASE
       WHEN buyer_document_id <> '' THEN buyer_document_id
       ELSE buyer_name
     END AS buyer_id,
     buyer_document_id,
     buyer_name,
     c,
     supplier_contract_count,
     supplier_value,
     supplier_low_competition_contract_count,
     supplier_direct_invitation_contract_count,
     supplier_execution_gap_contract_count,
     supplier_commitment_gap_contract_count,
     supplier_discrepancy_contract_count,
     supplier_discrepancy_value,
     supplier_has_sanction,
     supplier_official_overlap_count,
     supplier_capacity_mismatch_flag,
     coalesce(c.razon_social, c.name, c.document_id, c.nit) AS supplier_name,
     coalesce(c.document_id, c.nit) AS supplier_document_id
ORDER BY buyer_id ASC,
         supplier_value DESC,
         supplier_contract_count DESC,
         supplier_name ASC
WITH buyer_id,
     buyer_document_id,
     CASE
       WHEN buyer_name <> '' THEN buyer_name
       WHEN buyer_document_id <> '' THEN buyer_document_id
       ELSE 'Entidad sin nombre'
     END AS buyer_name,
     collect({
       name: supplier_name,
       document_id: supplier_document_id,
       value: supplier_value
     }) AS supplier_rows,
     sum(supplier_contract_count) AS contract_count,
     coalesce(sum(supplier_value), 0.0) AS contract_value,
     count(*) AS supplier_count,
     sum(supplier_low_competition_contract_count) AS low_competition_contract_count,
     sum(supplier_direct_invitation_contract_count) AS direct_invitation_contract_count,
     sum(supplier_execution_gap_contract_count) AS execution_gap_contract_count,
     sum(supplier_commitment_gap_contract_count) AS commitment_gap_contract_count,
     sum(
       CASE
         WHEN supplier_has_sanction THEN supplier_contract_count
         ELSE 0
       END
     ) AS sanctioned_supplier_contract_count,
     sum(
       CASE
         WHEN supplier_has_sanction THEN supplier_value
         ELSE 0.0
       END
     ) AS sanctioned_supplier_value,
     sum(
       CASE
         WHEN supplier_official_overlap_count > 0 THEN supplier_contract_count
         ELSE 0
       END
     ) AS official_overlap_contract_count,
     sum(
       CASE
         WHEN supplier_official_overlap_count > 0 THEN 1
         ELSE 0
       END
     ) AS official_overlap_supplier_count,
     sum(
       CASE
         WHEN supplier_capacity_mismatch_flag > 0 THEN 1
         ELSE 0
       END
     ) AS capacity_mismatch_supplier_count,
     sum(supplier_discrepancy_contract_count) AS discrepancy_contract_count,
     coalesce(sum(supplier_discrepancy_value), 0.0) AS discrepancy_value
WITH buyer_id,
     buyer_document_id,
     buyer_name,
     contract_count,
     contract_value,
     supplier_count,
     head(supplier_rows) AS top_supplier,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     official_overlap_contract_count,
     official_overlap_supplier_count,
     capacity_mismatch_supplier_count,
     execution_gap_contract_count,
     commitment_gap_contract_count,
     discrepancy_contract_count,
     discrepancy_value
CALL {
  WITH buyer_document_id, buyer_name
  OPTIONAL MATCH (buyer_entity:Company)-[:TIENE_HALLAZGO]->(finding:Finding)
  WHERE (
    buyer_document_id <> ''
    AND (
      buyer_entity.document_id = buyer_document_id
      OR buyer_entity.nit = buyer_document_id
    )
  ) OR (
    buyer_document_id = ''
    AND buyer_name <> ''
    AND toUpper(trim(coalesce(buyer_entity.razon_social, buyer_entity.name, ''))) =
        toUpper(trim(buyer_name))
  )
  RETURN count(DISTINCT finding) AS fiscal_finding_count,
         coalesce(sum(coalesce(finding.amount, 0.0)), 0.0) AS fiscal_finding_total
}
WITH buyer_id,
     buyer_document_id,
     buyer_name,
     contract_count,
     contract_value,
     supplier_count,
     top_supplier.name AS top_supplier_name,
     top_supplier.document_id AS top_supplier_document_id,
     CASE
       WHEN contract_value > 0.0 THEN toFloat(top_supplier.value) / contract_value
       ELSE 0.0
     END AS top_supplier_share,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     official_overlap_contract_count,
     official_overlap_supplier_count,
     capacity_mismatch_supplier_count,
     execution_gap_contract_count,
     commitment_gap_contract_count,
     discrepancy_contract_count,
     discrepancy_value,
     fiscal_finding_count,
     fiscal_finding_total
WITH buyer_id,
     buyer_document_id,
     buyer_name,
     contract_count,
     contract_value,
     supplier_count,
     top_supplier_name,
     top_supplier_document_id,
     top_supplier_share,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     official_overlap_contract_count,
     official_overlap_supplier_count,
     capacity_mismatch_supplier_count,
     execution_gap_contract_count,
     commitment_gap_contract_count,
     discrepancy_contract_count,
     discrepancy_value,
     fiscal_finding_count,
     fiscal_finding_total,
     (
       CASE WHEN top_supplier_share >= 0.35 THEN 1 ELSE 0 END +
       CASE WHEN low_competition_contract_count > 0 OR direct_invitation_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN sanctioned_supplier_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN official_overlap_supplier_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN capacity_mismatch_supplier_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN discrepancy_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN fiscal_finding_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types,
     (
       CASE
         WHEN fiscal_finding_count >= 5 THEN 4
         WHEN fiscal_finding_count >= 2 THEN 3
         WHEN fiscal_finding_count > 0 THEN 2
         ELSE 0
       END +
       CASE
         WHEN top_supplier_share >= 0.60 THEN 5
         WHEN top_supplier_share >= 0.45 THEN 4
         WHEN top_supplier_share >= 0.35 THEN 3
         ELSE 0
       END +
       CASE
         WHEN low_competition_contract_count >= 5 THEN 4
         WHEN low_competition_contract_count >= 2 THEN 3
         WHEN low_competition_contract_count > 0 THEN 2
         ELSE 0
       END +
       CASE
         WHEN sanctioned_supplier_contract_count >= 5 THEN 4
         WHEN sanctioned_supplier_contract_count >= 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN official_overlap_supplier_count >= 3 THEN 4
         WHEN official_overlap_supplier_count >= 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN capacity_mismatch_supplier_count >= 3 THEN 3
         WHEN capacity_mismatch_supplier_count >= 1 THEN 2
         ELSE 0
       END +
       CASE
         WHEN discrepancy_contract_count >= 5 THEN 4
         WHEN discrepancy_contract_count >= 1 THEN 3
         ELSE 0
       END +
       CASE
         WHEN contract_count >= 20 THEN 2
         WHEN contract_count >= 10 THEN 1
         ELSE 0
       END
     ) AS suspicion_score
WHERE signal_types >= 1
RETURN buyer_id,
       buyer_name,
       CASE WHEN buyer_document_id <> '' THEN buyer_document_id ELSE null END AS buyer_document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(contract_count) AS contract_count,
       toFloat(contract_value) AS contract_value,
       toInteger(supplier_count) AS supplier_count,
       top_supplier_name,
       top_supplier_document_id,
       toFloat(top_supplier_share) AS top_supplier_share,
       toInteger(low_competition_contract_count) AS low_competition_contract_count,
       toInteger(direct_invitation_contract_count) AS direct_invitation_contract_count,
       toInteger(sanctioned_supplier_contract_count) AS sanctioned_supplier_contract_count,
       toFloat(sanctioned_supplier_value) AS sanctioned_supplier_value,
       toInteger(official_overlap_contract_count) AS official_overlap_contract_count,
       toInteger(official_overlap_supplier_count) AS official_overlap_supplier_count,
       toInteger(capacity_mismatch_supplier_count) AS capacity_mismatch_supplier_count,
       toInteger(fiscal_finding_count) AS fiscal_finding_count,
       toFloat(fiscal_finding_total) AS fiscal_finding_total,
       toInteger(execution_gap_contract_count) AS execution_gap_contract_count,
       toInteger(commitment_gap_contract_count) AS commitment_gap_contract_count,
       toInteger(discrepancy_contract_count) AS discrepancy_contract_count,
       toFloat(discrepancy_value) AS discrepancy_value
ORDER BY suspicion_score DESC,
         top_supplier_share DESC,
         contract_value DESC,
         sanctioned_supplier_contract_count DESC,
         buyer_name ASC
LIMIT toInteger($limit)
