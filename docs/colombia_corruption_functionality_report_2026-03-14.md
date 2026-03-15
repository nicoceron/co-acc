# Colombia Corruption Functionality Report

Date: **March 14, 2026**

## Current Live State

- API registry stats: `30` implemented sources, `23` loaded sources, `1` blocked-external source
- Live graph totals: `4,749,820` nodes, `8,410,725` relationships
- Newly added in this pass:
  - `rues_chambers` implemented and loaded
  - `registraduria_death_status_checks` implemented as a manual-import source contract

## Working Corruption Functionality

### Live in the product

- Suspicious people watchlist in the dashboard
- Suspicious companies watchlist in the dashboard
- Company and person analysis pages with readable case summaries
- Supplier sanction overlays from `SECOP` and `PACO`
- Public-official and supplier overlap signals from `SIGEP` plus contracting data
- Low-competition bid signals from `SECOP II ofertas`
- Budget-chain gap signals from:
  - `SECOP II compromisos presupuestales`
  - `SECOP II solicitudes CDP`
  - `SECOP II facturas`
  - `SECOP II ejecución`
  - `SECOP II ubicaciones de ejecución`
- Company financial-capacity enrichment from `Supersociedades`
- Project and territory context from `MapaInversiones` and `SGR`
- Property-market context from `IGAC` transaction aggregates

### Graph layers now present

- People: officials, candidates, donors, public servants, legal representatives
- Organizations: public entities, companies, chambers of commerce
- Money events: procurement contracts, invoices, commitments, finance records, project values
- Places: departments, municipalities, project territories
- Documents/signals: sanctions, conflict disclosures, asset disclosures, identity-status checks schema

## Source Coverage

### Implemented and loaded

- `SECOP` core, auxiliary contracting, offers, invoices, commitments, execution, sanctions, suppliers
- `SIGEP` public servants and sensitive positions
- `Ley 2013` asset and conflict disclosures
- `SGR` projects and expense execution
- `PACO`
- `PTE`
- `MapaInversiones`
- `Supersociedades top companies`
- `RUES chambers`

### Implemented but not yet loaded or only manual

- `IGAC property transactions`: pipeline implemented, local backfill not completed in this pass
- `Registraduria death status checks`: normalized import pipeline implemented; automated bulk collection still blocked by the public site

## Partial or Missing Detection Areas

- `Budget leakage`: partial, because the spending-chain sources are in place but the alert rules are still limited
- `Bid-rigging`: partial, because low-competition logic is live but shared-contact / winner-rotation modeling is not
- `Officeholder/contractor overlap`: partial, live for overlap scoring but not yet expanded into full temporal incompatibility rules
- `Company-capacity mismatch`: data layer is live via `Supersociedades`, but the dedicated pattern rule is not yet live
- `Relative/conflict enrichment`: partial, via `Ley 2013`; deeper `RUES/RUP` control data is still incomplete
- `Land-benefit around public works`: partial, because `IGAC` is modeled as market activity rather than owner-level proof
- `Ghost payroll`: not complete; this still requires payroll / HR / payment data and real Registraduria or social-protection checks

## Hard Access Limits

- `Registraduria` public consultation is protected by anti-bot gating, so nationwide automated collection is not currently a clean open-data ingest path
- `RUES/RUP` deeper company-control data remains only partially accessible from public endpoints
- `DIAN tax declarations` and `RUB beneficial ownership` are still access-restricted and not part of the open-data build

