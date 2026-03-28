// Baseline: peer comparison by procuring organization
// Compares a supplier's procurement totals against peers serving the same buyer
MATCH (buyer:Company)-[award:CONTRATOU]->(co:Company)
WHERE coalesce(award.buyer_name, buyer.razon_social, buyer.name) IS NOT NULL
  AND ($entity_id IS NULL OR elementId(co) = $entity_id)
WITH co,
     coalesce(award.buyer_name, buyer.razon_social, buyer.name) AS region,
     sum(coalesce(award.contract_count, 0)) AS contract_count,
     sum(coalesce(award.total_value, 0.0)) AS total_value

MATCH (peer_buyer:Company)-[peer_award:CONTRATOU]->(peer:Company)
WHERE coalesce(peer_award.buyer_name, peer_buyer.razon_social, peer_buyer.name) = region
WITH region, co, contract_count, total_value,
     COUNT(DISTINCT peer) AS region_companies,
     sum(coalesce(peer_award.contract_count, 0)) AS region_contracts,
     sum(coalesce(peer_award.total_value, 0.0)) AS region_total_value
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
