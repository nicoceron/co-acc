# Candidate dataset shortlist - March 25, 2026

This shortlist is the manual review layer on top of the generated probe report in
[`candidate_dataset_probe_2026-03-25.md`](./candidate_dataset_probe_2026-03-25.md).

The probe report answers one question: "does this source expose exact keys that
hit the current graph universe?" This shortlist answers the harder question:
"is it worth implementing next, or is it duplicate/noise?"

## Implement next

- `3xwx-53wt` `SECOP I - Origen de los Recursos`
  - Strong BPIN overlap against the live graph.
  - Useful for tracing public-money channel origin, especially once linked with
    SECOP I historical processes and BPIN/project layers.

- `c82u-588k` `Personas Naturales, Personas Jurídicas y Entidades Sin Ánimo de Lucro`
  - Strong company and legal-representative identifier overlap.
  - Best candidate for real company-control and representative normalization.

- `nb3d-v3n7` `Establecimientos - Agencias - Sucursales`
  - Strong company identifier overlap.
  - Useful for branch / establishment expansion around supplier networks.

- `qddk-cgux` `SECOP I - Procesos de Compra Pública Histórico`
  - Strong contractor-id overlap and meaningful BPIN traces.
  - Worth implementing as the main SECOP I historical backbone.

- `wwhe-4sq8` `SECOP II - Ubicaciones Adicionales`
  - Strong contract-id and entity-id overlap.
  - Useful as a real contract-location companion, not just a generic geography feed.

- `epzv-8ck4` `DNP-EntidadEjecutoraProyecto`
- `iuc2-3r6h` `DNP-BeneficiariosProyectoLocalizacion`
- `tmmn-mpqc` `DNP-BeneficiariosProyectoCaracterizacion`
- `xikz-44ja` `DNP-LocalizacionProyecto`
  - All four close through BPIN with the current graph.
  - Worth implementing together as the next DNP/BPIN expansion wave.

## Enrichment only

- `bxkb-7j6i` `Trámites Ambientales`
  - Real NIT overlap exists, but it is still thin in the current procurement graph.
  - Keep as enrichment or investigation support, not as a promoted pattern layer yet.

## Companion / duplicate / quarantine

- `5p2a-fyvn` `Vista SECOP II - Ubicaciones ejecución contratos`
  - The probe hits, but this behaves like a companion view to the already useful
    execution-location source rather than a new backbone layer.
  - Do not prioritize ahead of distinct datasets.

- `niuu-28bi` `Contratación Efectuada en el año 2021`
  - Small local contracting feed with some exact overlaps.
  - Keep only as a local feeder if a case requires it.

- `xnsw-bdfj` `DOSIS APLICADAS CONTRA COVID-19 -AÑO 2021`
  - Probe confirms the earlier suspicion: this is effectively contracting-shaped data
    under a misleading title.
  - Quarantine it instead of promoting it.

- `wtyw-nhcv` `Presupuesto de Gastos del Sistema General de Regalías (SGR) Histórico`
  - Some BPIN overlap exists, but the current signal is thin.
  - Hold until the SGR project universe is widened further.

## Weak or drop for now

- Pension aggregates:
  - `gnut-8jsz`, `hdnf-a76p`, `j3e8-4hke`, `jvtd-3dgy`, `np5z-haxm`, `p4st-2k4t`, `yjvt-9cab`
  - These remain aggregate/statistical, not person-level fraud joins.

- Mining / RUCOM / local environmental feeds:
  - `42ha-fhvj`, `74ct-m5y8`, `7amp-4swy`, `7h9i-7gun`, `acrw-g46v`, `f385-sqmw`,
    `mnk6-hfcu`, `si2v-pbq5`, `xjzv-xx6n`, `xzu3-gnau`
  - Mostly weak name/territory overlap and no meaningful contract/control closure today.

- Local roster / local permits / local SECOP variants:
  - `7fsy-xzzb`, `g75e-9nxr`, `n686-d6yb`, `y524-had9`
  - Not enough real exact-id overlap to justify implementation now.

- SECOP companions with weak closure:
  - `ityv-bxct`, `sqpp-4gyj`, `tauh-5jvn`, `u5b4-ae3s`
  - Keep out unless a later probe shows stronger distinct value than the sources
    already loaded in the graph.
