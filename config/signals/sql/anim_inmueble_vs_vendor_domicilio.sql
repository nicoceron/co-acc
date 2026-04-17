WITH matches AS (
  SELECT
    coalesce(nullif(a.property_id, ''), nullif(s.domicile_property_id, ''), nullif(a.address_key, ''), nullif(s.address_key, '')) AS property_key,
    coalesce(nullif(s.municipality, ''), nullif(a.municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(s.department, ''), 'NACIONAL') AS department,
    s.supplier_document_id
  FROM src_secop_suppliers s
  JOIN src_anim_inmuebles a
    ON (s.domicile_property_id <> '' AND s.domicile_property_id = a.property_id)
    OR (s.address_key <> '' AND s.address_key = a.address_key)
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'anim_inmueble_vs_vendor_domicilio:' || property_key AS scope_key,
  count(DISTINCT supplier_document_id) AS risk_signal,
  count(DISTINCT supplier_document_id) AS evidence_count,
  [property_key] AS evidence_refs,
  municipality,
  department,
  property_key,
  count(DISTINCT supplier_document_id) AS supplier_count
FROM matches
GROUP BY property_key, municipality, department
HAVING count(DISTINCT supplier_document_id) > 0;
