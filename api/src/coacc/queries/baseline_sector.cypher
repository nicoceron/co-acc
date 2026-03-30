// Baseline: peer comparison by Colombian CIIU sector
// Compares a supplier's procurement metrics against sector average
WITH replace(trim(coalesce($entity_id, '')), '-', '') AS lookup_id
MATCH (:Company)-[award:CONTRATOU]->(co:Company)
WHERE coalesce(co.primary_ciiu_code, co.ciiu4, co.secondary_ciiu_code) IS NOT NULL
  AND (
    lookup_id = ''
    OR elementId(co) = $entity_id
    OR replace(coalesce(co.document_id, ''), '-', '') = lookup_id
    OR replace(coalesce(co.nit, ''), '-', '') = lookup_id
  )
WITH co,
     sum(coalesce(award.contract_count, 0)) AS contract_count,
     sum(coalesce(award.total_value, 0.0)) AS total_value,
     coalesce(co.primary_ciiu_code, co.ciiu4, co.secondary_ciiu_code) AS ciiu
WITH ciiu, co, contract_count, total_value

MATCH (:Company)-[pc:CONTRATOU]->(peer:Company)
WHERE coalesce(peer.primary_ciiu_code, peer.ciiu4, peer.secondary_ciiu_code) = ciiu
WITH ciiu, co, contract_count, total_value,
     COUNT(DISTINCT peer) AS sector_companies,
     sum(coalesce(pc.contract_count, 0)) AS sector_contracts,
     sum(coalesce(pc.total_value, 0.0)) AS sector_total_value
WITH ciiu, co, contract_count, total_value,
     sector_companies,
     toFloat(sector_contracts) / CASE WHEN sector_companies > 0
       THEN toFloat(sector_companies) ELSE 1.0 END AS avg_contracts,
     toFloat(sector_total_value) / CASE WHEN sector_companies > 0
       THEN toFloat(sector_companies) ELSE 1.0 END AS avg_value
RETURN co.razon_social AS company_name,
       coalesce(co.document_id, co.nit) AS company_document_id,
       elementId(co) AS company_id,
       ciiu AS sector_ciiu,
       contract_count,
       total_value,
       sector_companies,
       avg_contracts AS sector_avg_contracts,
       avg_value AS sector_avg_value,
       toFloat(contract_count) / CASE WHEN avg_contracts > 0
         THEN avg_contracts ELSE 1.0 END AS contract_ratio,
       toFloat(total_value) / CASE WHEN avg_value > 0
         THEN avg_value ELSE 1.0 END AS value_ratio
ORDER BY value_ratio DESC
LIMIT 50
