# Colombia candidate dataset probe

This report was generated from official `datos.gov.co` metadata and sample rows, then probed against the current live graph universe using normalized contract, process, company, person, and BPIN keys.

## Implement Next

### dmgg-8hin - SECOP II - Archivos Descarga Desde 2025

- Rows: `None`
- Families: `company_id, company_name, process_id`
- Reason: Connected live SoQL probes hit the current graph universe: process_id:proceso:308, company_id:nit_entidad:5351581
- Errors: `count failed: The read operation timed out`
- company_id: `nit_entidad sample=205/206 live=5351581`
- company_name: `nombre_archivo sample=0/292`
- process_id: `proceso sample=2/297 live=308`

## Enrichment Only

_None._

## Weak Feeder

_None._

## Drop

### ps88-5e3v - SECOP I - Archivos Descarga

- Rows: `32389126`
- Families: `contract_id`
- Reason: No meaningful exact-id overlap with the current graph universe was detected.
- contract_id: `numero_de_constancia sample=0/300 live=0`
