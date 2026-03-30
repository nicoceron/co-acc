// Baseline: peer comparison by procuring organization
// Compares a supplier's procurement totals against peers serving the same buyer
WITH replace(trim(coalesce($entity_id, '')), '-', '') AS lookup_id
MATCH (buyer:Company)-[award:CONTRATOU]->(co:Company)
WHERE coalesce(award.buyer_name, buyer.razon_social, buyer.name) IS NOT NULL
  AND (
    lookup_id = ''
    OR elementId(co) = $entity_id
    OR replace(coalesce(co.document_id, ''), '-', '') = lookup_id
    OR replace(coalesce(co.nit, ''), '-', '') = lookup_id
  )
WITH co,
     buyer,
     coalesce(award.buyer_name, buyer.razon_social, buyer.name) AS region,
     sum(coalesce(award.contract_count, 0)) AS contract_count,
     sum(coalesce(award.total_value, 0.0)) AS total_value
CALL {
  WITH buyer
  MATCH (buyer)-[peer_award:CONTRATOU]->(peer:Company)
  RETURN COUNT(DISTINCT peer) AS region_companies,
         sum(coalesce(peer_award.contract_count, 0)) AS region_contracts,
         sum(coalesce(peer_award.total_value, 0.0)) AS region_total_value
}
WITH region, co, contract_count, total_value,
     region_companies, region_contracts, region_total_value
WITH region, co, contract_count, total_value,
     region_companies,
     toFloat(region_contracts) / CASE WHEN region_companies > 0
       THEN toFloat(region_companies) ELSE 1.0 END AS avg_contracts,
     toFloat(region_total_value) / CASE WHEN region_companies > 0
       THEN toFloat(region_companies) ELSE 1.0 END AS avg_value
RETURN co.razon_social AS company_name,
       coalesce(co.document_id, co.nit) AS company_document_id,
       elementId(co) AS company_id,
       region,
       contract_count,
       total_value,
       region_companies,
       avg_contracts AS region_avg_contracts,
       avg_value AS region_avg_value,
       toFloat(contract_count) / CASE WHEN avg_contracts > 0
         THEN avg_contracts ELSE 1.0 END AS contract_ratio,
       toFloat(total_value) / CASE WHEN avg_value > 0
         THEN avg_value ELSE 1.0 END AS value_ratio
ORDER BY value_ratio DESC
LIMIT 50
