MATCH ()-[award:CONTRATOU]->(c:Company)
WITH trim(coalesce(award.department, '')) AS department,
     trim(coalesce(award.city, '')) AS municipality,
     c,
     count(DISTINCT award.summary_id) AS supplier_contract_count,
     coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS supplier_value,
     count(DISTINCT coalesce(award.buyer_document_id, award.buyer_name)) AS supplier_buyer_count,
     count(
       DISTINCT CASE
         WHEN coalesce(award.offer_count, 0) <= 2 OR coalesce(award.direct_invitation, false)
         THEN award.summary_id
         ELSE null
       END
     ) AS supplier_low_competition_contract_count,
     count(
       DISTINCT CASE
         WHEN coalesce(award.direct_invitation, false)
         THEN award.summary_id
         ELSE null
       END
     ) AS supplier_direct_invitation_contract_count,
     count(
       DISTINCT CASE
         WHEN (
           coalesce(award.invoice_total_value, 0.0) > 0.0
           AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
         ) OR (
           coalesce(award.commitment_total_value, 0.0) > 0.0
           AND coalesce(award.invoice_total_value, 0.0) >
               coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
         )
         THEN award.summary_id
         ELSE null
       END
     ) AS supplier_discrepancy_contract_count,
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
WHERE department <> '' OR municipality <> ''
WITH CASE
       WHEN municipality <> '' AND department <> '' THEN municipality + '|' + department
       WHEN municipality <> '' THEN municipality
       ELSE department
     END AS territory_id,
     CASE
       WHEN municipality <> '' AND department <> '' THEN municipality + ", " + department
       WHEN municipality <> '' THEN municipality
       ELSE department
     END AS territory_name,
     CASE
       WHEN department <> '' THEN department
       ELSE municipality
     END AS normalized_department,
     CASE
       WHEN municipality <> '' THEN municipality
       ELSE null
     END AS normalized_municipality,
     c,
     supplier_contract_count,
     supplier_value,
     supplier_buyer_count,
     supplier_low_competition_contract_count,
     supplier_direct_invitation_contract_count,
     supplier_discrepancy_contract_count,
     supplier_discrepancy_value,
     supplier_has_sanction,
     coalesce(c.razon_social, c.name, c.document_id, c.nit, c.cnpj) AS supplier_name
ORDER BY territory_id ASC,
         supplier_value DESC,
         supplier_contract_count DESC,
         supplier_name ASC
WITH territory_id,
     territory_name,
     normalized_department AS department,
     normalized_municipality AS municipality,
     collect({
       name: supplier_name,
       value: supplier_value
     }) AS supplier_rows,
     sum(supplier_contract_count) AS contract_count,
     coalesce(sum(supplier_value), 0.0) AS contract_value,
     coalesce(sum(supplier_buyer_count), 0) AS buyer_count,
     count(*) AS supplier_count,
     sum(supplier_low_competition_contract_count) AS low_competition_contract_count,
     sum(supplier_direct_invitation_contract_count) AS direct_invitation_contract_count,
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
     sum(supplier_discrepancy_contract_count) AS discrepancy_contract_count,
     coalesce(sum(supplier_discrepancy_value), 0.0) AS discrepancy_value
WITH territory_id,
     territory_name,
     department,
     municipality,
     contract_count,
     contract_value,
     buyer_count,
     supplier_count,
     head(supplier_rows) AS top_supplier,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     0 AS official_overlap_contract_count,
     0 AS capacity_mismatch_supplier_count,
     discrepancy_contract_count,
     discrepancy_value
WITH territory_id,
     territory_name,
     department,
     municipality,
     contract_count,
     contract_value,
     buyer_count,
     supplier_count,
     top_supplier.name AS top_supplier_name,
     CASE
       WHEN contract_value > 0.0 THEN toFloat(top_supplier.value) / contract_value
       ELSE 0.0
     END AS top_supplier_share,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     official_overlap_contract_count,
     capacity_mismatch_supplier_count,
     discrepancy_contract_count,
     discrepancy_value
WITH territory_id,
     territory_name,
     department,
     municipality,
     contract_count,
     contract_value,
     buyer_count,
     supplier_count,
     top_supplier_name,
     top_supplier_share,
     low_competition_contract_count,
     direct_invitation_contract_count,
     sanctioned_supplier_contract_count,
     sanctioned_supplier_value,
     official_overlap_contract_count,
     capacity_mismatch_supplier_count,
     discrepancy_contract_count,
     discrepancy_value,
     (
       CASE WHEN top_supplier_share >= 0.35 THEN 1 ELSE 0 END +
       CASE WHEN low_competition_contract_count > 0 OR direct_invitation_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN sanctioned_supplier_contract_count > 0 THEN 1 ELSE 0 END +
       CASE WHEN discrepancy_contract_count > 0 THEN 1 ELSE 0 END
     ) AS signal_types,
     (
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
RETURN territory_id,
       territory_name,
       department,
       municipality,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(contract_count) AS contract_count,
       toFloat(contract_value) AS contract_value,
       toInteger(buyer_count) AS buyer_count,
       toInteger(supplier_count) AS supplier_count,
       top_supplier_name,
       toFloat(top_supplier_share) AS top_supplier_share,
       toInteger(low_competition_contract_count) AS low_competition_contract_count,
       toInteger(direct_invitation_contract_count) AS direct_invitation_contract_count,
       toInteger(sanctioned_supplier_contract_count) AS sanctioned_supplier_contract_count,
       toFloat(sanctioned_supplier_value) AS sanctioned_supplier_value,
       toInteger(official_overlap_contract_count) AS official_overlap_contract_count,
       toInteger(capacity_mismatch_supplier_count) AS capacity_mismatch_supplier_count,
       toInteger(discrepancy_contract_count) AS discrepancy_contract_count,
       toFloat(discrepancy_value) AS discrepancy_value
ORDER BY suspicion_score DESC,
         top_supplier_share DESC,
         contract_value DESC,
         sanctioned_supplier_contract_count DESC,
         territory_name ASC
LIMIT toInteger($limit)
