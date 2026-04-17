WITH supplier_parcels AS (
  SELECT
    coalesce(nullif(s.parcel_id, ''), nullif(i.parcel_id, ''), nullif(s.address_key, ''), nullif(i.address_key, '')) AS parcel_key,
    coalesce(nullif(s.municipality, ''), nullif(i.municipality, ''), 'NACIONAL') AS municipality,
    coalesce(nullif(s.department, ''), 'NACIONAL') AS department,
    s.supplier_document_id
  FROM src_secop_suppliers s
  JOIN src_igac_parcelas i
    ON (s.parcel_id <> '' AND s.parcel_id = i.parcel_id)
    OR (s.address_key <> '' AND s.address_key = i.address_key)
)
SELECT
  'territory:' || municipality AS entity_id,
  'territory:' || municipality AS entity_key,
  'Territory' AS entity_label,
  'catastro_igac_vs_vendor_address:' || parcel_key AS scope_key,
  count(DISTINCT supplier_document_id) AS risk_signal,
  count(DISTINCT supplier_document_id) AS evidence_count,
  [parcel_key] AS evidence_refs,
  municipality,
  department,
  parcel_key,
  count(DISTINCT supplier_document_id) AS supplier_count
FROM supplier_parcels
GROUP BY parcel_key, municipality, department
HAVING count(DISTINCT supplier_document_id) >= 2;
