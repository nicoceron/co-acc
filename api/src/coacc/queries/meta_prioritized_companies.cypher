CALL {
  MATCH (c:Company)-[:SANCIONADA]->(s:Sanction)
  RETURN elementId(c) AS company_id,
         c AS company,
         count(DISTINCT s) AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
  UNION ALL
  MATCH (p:Person)-[:OFFICER_OF]->(c:Company)
  WHERE EXISTS { MATCH (p)-[:RECIBIO_SALARIO]->(:PublicOffice) }
  OPTIONAL MATCH (p)-[:RECIBIO_SALARIO]->(o:PublicOffice)
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         count(DISTINCT p) AS official_officer_count,
         count(DISTINCT o) AS official_role_count,
         collect(DISTINCT coalesce(p.name, p.nombre, p.document_id))[0..3] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
  UNION ALL
  MATCH (c:Company)-[offer:SUMINISTRO_LICITACAO]->(b:Bid)
  WHERE coalesce(b.offer_count, 0) <= 2
     OR coalesce(b.direct_invitation, false) = true
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         count(DISTINCT b) AS low_competition_bid_count,
         coalesce(sum(coalesce(offer.offer_value_total, 0.0)), 0.0) AS low_competition_bid_value,
         count(DISTINCT CASE WHEN coalesce(b.direct_invitation, false) THEN b END) AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
  UNION ALL
  MATCH ()-[award:CONTRATOU]->(c:Company)
  WHERE coalesce(award.invoice_total_value, 0.0) > 0
    AND coalesce(award.execution_actual_progress_max, 0.0) < 25.0
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         count(DISTINCT award.summary_id) AS execution_gap_contract_count,
         coalesce(sum(coalesce(award.invoice_total_value, 0.0)), 0.0) AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
  UNION ALL
  MATCH ()-[award:CONTRATOU]->(c:Company)
  WHERE coalesce(award.commitment_total_value, 0.0) > 0
    AND coalesce(award.invoice_total_value, 0.0) >
        coalesce(award.commitment_total_value, 0.0) * (1.0 + toFloat($pattern_min_discrepancy_ratio))
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         count(DISTINCT award.summary_id) AS commitment_gap_contract_count,
         coalesce(
           sum(
             coalesce(award.invoice_total_value, 0.0) -
             coalesce(award.commitment_total_value, 0.0)
           ),
           0.0
         ) AS commitment_gap_total
  UNION ALL
  MATCH (c:Company)-[:SUMINISTRO]->(f:Finance {type: 'SGR_EXPENSE_EXECUTION'})
  WHERE coalesce(c.document_id, c.nit, c.cnpj, '') <> '0'
    AND EXISTS { MATCH ()-[:CONTRATOU]->(c) }
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         count(DISTINCT f) AS funding_overlap_event_count,
         coalesce(sum(coalesce(f.value, 0.0)), 0.0) AS funding_overlap_total,
         0 AS capacity_mismatch_contract_count,
         0.0 AS capacity_mismatch_contract_value,
         0.0 AS capacity_mismatch_revenue_ratio,
         0.0 AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
  UNION ALL
  MATCH (c:Company)-[:DECLARO_FINANZAS]->(f:Finance {type: 'SUPERSOC_TOP_COMPANY'})
  CALL {
    WITH c
    MATCH ()-[award:CONTRATOU]->(c)
    RETURN count(DISTINCT award.summary_id) AS contract_count,
           coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_total
  }
  WITH c,
       contract_count,
       contract_total,
       CASE
         WHEN coalesce(f.operating_revenue_current, 0.0) > 0.0
         THEN contract_total / toFloat(f.operating_revenue_current)
         ELSE 0.0
       END AS revenue_ratio,
       CASE
         WHEN coalesce(f.total_assets_current, 0.0) > 0.0
         THEN contract_total / toFloat(f.total_assets_current)
         ELSE 0.0
       END AS asset_ratio
  WHERE contract_total >= toFloat($pattern_min_discrepancy_ratio) * 1000000000
    AND (
      revenue_ratio >= 2.0
      OR asset_ratio >= 1.0
    )
  RETURN elementId(c) AS company_id,
         c AS company,
         0 AS sanction_count,
         0 AS official_officer_count,
         0 AS official_role_count,
         [] AS official_names,
         0 AS low_competition_bid_count,
         0.0 AS low_competition_bid_value,
         0 AS direct_invitation_bid_count,
         0 AS funding_overlap_event_count,
         0.0 AS funding_overlap_total,
         contract_count AS capacity_mismatch_contract_count,
         contract_total AS capacity_mismatch_contract_value,
         toFloat(revenue_ratio) AS capacity_mismatch_revenue_ratio,
         toFloat(asset_ratio) AS capacity_mismatch_asset_ratio,
         0 AS execution_gap_contract_count,
         0.0 AS execution_gap_invoice_total,
         0 AS commitment_gap_contract_count,
         0.0 AS commitment_gap_total
}
WITH company_id,
     head(collect(company)) AS c,
     sum(sanction_count) AS sanction_count,
     sum(official_officer_count) AS official_officer_count,
     sum(official_role_count) AS official_role_count,
     head([names IN collect(official_names) WHERE size(names) > 0]) AS official_names,
     sum(low_competition_bid_count) AS low_competition_bid_count,
     sum(low_competition_bid_value) AS low_competition_bid_value,
     sum(direct_invitation_bid_count) AS direct_invitation_bid_count,
     sum(funding_overlap_event_count) AS funding_overlap_event_count,
     sum(funding_overlap_total) AS funding_overlap_total,
     max(capacity_mismatch_contract_count) AS capacity_mismatch_contract_count,
     max(capacity_mismatch_contract_value) AS capacity_mismatch_contract_value,
     max(capacity_mismatch_revenue_ratio) AS capacity_mismatch_revenue_ratio,
     max(capacity_mismatch_asset_ratio) AS capacity_mismatch_asset_ratio,
     sum(execution_gap_contract_count) AS execution_gap_contract_count,
     sum(execution_gap_invoice_total) AS execution_gap_invoice_total,
     sum(commitment_gap_contract_count) AS commitment_gap_contract_count,
     sum(commitment_gap_total) AS commitment_gap_total,
     count(*) AS signal_types
WHERE signal_types >= 1
CALL {
  WITH c
  MATCH ()-[award:CONTRATOU]->(c)
  RETURN count(DISTINCT award.summary_id) AS contract_count,
         coalesce(sum(coalesce(award.total_value, 0.0)), 0.0) AS contract_value,
         count(DISTINCT award.buyer_document_id) AS buyer_count
}
WITH c,
     contract_count,
     contract_value,
     buyer_count,
     sanction_count,
     official_officer_count,
     official_role_count,
     coalesce(official_names, []) AS official_names,
     low_competition_bid_count,
     low_competition_bid_value,
     direct_invitation_bid_count,
     funding_overlap_event_count,
     funding_overlap_total,
     capacity_mismatch_contract_count,
     capacity_mismatch_contract_value,
     capacity_mismatch_revenue_ratio,
     capacity_mismatch_asset_ratio,
     execution_gap_contract_count,
     execution_gap_invoice_total,
     commitment_gap_contract_count,
     commitment_gap_total,
     signal_types,
     (
       CASE
         WHEN sanction_count >= 10 THEN 6
         WHEN sanction_count >= 3 THEN 5
         WHEN sanction_count > 0 THEN 4
         ELSE 0
       END +
       CASE WHEN official_officer_count > 0 THEN 4 ELSE 0 END +
       CASE
         WHEN low_competition_bid_count >= 5 THEN 3
         WHEN low_competition_bid_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN funding_overlap_event_count >= 20 THEN 3
         WHEN funding_overlap_event_count >= 5 THEN 2
         WHEN funding_overlap_event_count > 0 THEN 1
         ELSE 0
       END +
       CASE
         WHEN capacity_mismatch_revenue_ratio >= 10 OR capacity_mismatch_asset_ratio >= 5 THEN 4
         WHEN capacity_mismatch_revenue_ratio >= 5 OR capacity_mismatch_asset_ratio >= 2 THEN 3
         WHEN capacity_mismatch_revenue_ratio >= 2 OR capacity_mismatch_asset_ratio >= 1 THEN 2
         ELSE 0
       END +
       CASE
         WHEN execution_gap_contract_count >= 5 THEN 3
         WHEN execution_gap_contract_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN commitment_gap_contract_count >= 5 THEN 3
         WHEN commitment_gap_contract_count >= 2 THEN 2
         ELSE 0
       END +
       CASE
         WHEN contract_count >= 10 THEN 2
         WHEN contract_count >= 3 THEN 1
         ELSE 0
       END
     ) AS suspicion_score
RETURN elementId(c) AS entity_id,
       coalesce(c.razon_social, c.name, c.document_id, c.nit) AS name,
       coalesce(c.document_id, c.nit, c.cnpj) AS document_id,
       toInteger(suspicion_score) AS suspicion_score,
       toInteger(signal_types) AS signal_types,
       toInteger(contract_count) AS contract_count,
       toFloat(contract_value) AS contract_value,
       toInteger(buyer_count) AS buyer_count,
       toInteger(sanction_count) AS sanction_count,
       toInteger(official_officer_count) AS official_officer_count,
       toInteger(official_role_count) AS official_role_count,
       official_names AS official_names,
       toInteger(low_competition_bid_count) AS low_competition_bid_count,
       toFloat(low_competition_bid_value) AS low_competition_bid_value,
       toInteger(direct_invitation_bid_count) AS direct_invitation_bid_count,
       toInteger(funding_overlap_event_count) AS funding_overlap_event_count,
       toFloat(funding_overlap_total) AS funding_overlap_total,
       toInteger(capacity_mismatch_contract_count) AS capacity_mismatch_contract_count,
       toFloat(capacity_mismatch_contract_value) AS capacity_mismatch_contract_value,
       toFloat(capacity_mismatch_revenue_ratio) AS capacity_mismatch_revenue_ratio,
       toFloat(capacity_mismatch_asset_ratio) AS capacity_mismatch_asset_ratio,
       toInteger(execution_gap_contract_count) AS execution_gap_contract_count,
       toFloat(execution_gap_invoice_total) AS execution_gap_invoice_total,
       toInteger(commitment_gap_contract_count) AS commitment_gap_contract_count,
       toFloat(commitment_gap_total) AS commitment_gap_total
ORDER BY suspicion_score DESC,
         capacity_mismatch_revenue_ratio DESC,
         capacity_mismatch_asset_ratio DESC,
         contract_value DESC,
         sanction_count DESC,
         low_competition_bid_count DESC,
         name ASC
LIMIT toInteger($limit)
