# SECOP Dataset Manual Review

Systematic review of every SECOP-related dataset in the triage, including those already in the pipeline (current_registry) and all candidates. Goal: determine which datasets to ingest, which are redundant, and which need custom join-key handling.

---

## 1. SECOP Datasets Already in the Pipeline (current_registry)

These are already wired into `source_registry_co_v1.csv` and/or `runner.py`. Listed with their registry status.

### 1.1 Core contracts

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_ii_contracts | jbjy-vk9h | SECOP II - Contratos Electrónicos | 5,605,050 | **Core**. 20 signal deps, 19 used. The anchor dataset. |
| secop_integrado | rpmr-utcd | SECOP Integrado | ? | **Core**. 9 signal deps, 2840 evidence refs. Historical merged view. |
| secop_i_historical_processes | qddk-cgux | SECOP I - Procesos de Compra Pública Histórico | ? | **Core**. 1098 evidence refs. Pre-2015 SECOP I process data. |

### 1.2 Contract lifecycle

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_contract_additions | cb9c-h8sn | SECOP II - Adiciones | 17,286,227 | **Keep**. 2 signal deps. Join key: `id_contrato`. |
| secop_contract_suspensions | u99c-7mfm | SECOP II - Suspensiones de Contratos | 443,154 | **Keep**. 1 signal dep. Join key: `id_contrato`. |
| secop_invoices | ibyt-yi2f | SECOP II - Facturas | 20,206,223 | **Keep**. 1 signal dep. Join keys: `id_contrato`, `codigo_entidad`. |
| secop_contract_execution | mfmm-jqmq | SECOP II - Ejecución Contratos | 4,394,443 | **Keep**. 2 signal deps. **No recognized join key** (see §3.1). |
| secop_contract_modifications | u8cx-r425 | SECOP II - Modificaciones a contratos | ? | **Keep**. 1 signal dep. Join key: `id_contrato`. |
| secop_payment_plans | uymx-8p3j | SECOP II - Plan de pagos | ? | **Keep_context**. 1 signal dep. Join key: `id_del_contrato` → `id_contrato`. |

### 1.3 Process / entity / procurement

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_ii_processes | p6dx-8zbt | SECOP II - Procesos de Contratación | 8,411,769 | **Keep**. 4 signal deps, 3 used. Join keys: `nit_entidad`, `codigo_entidad`. |
| secop_cdp_requests | a86w-fh92 | SECOP II - Solicitudes CDPs | 9,635,254 | **Keep**. 0 signal deps, 27 evidence refs. Join keys: `id_contrato`, `nit`, `nit_proveedor`, `id_proceso`. |
| secop_process_bpin | d9na-abhe | SECOP II - BPIN por Proceso | ? | **Keep**. 5 signal deps, 3 used. Join keys: `codigo_bpin`, `id_proceso`, `id_contracto`. |
| secop_offers | wi7w-2nvm | SECOP II - Ofertas Por Proceso | -1 | **Keep**. 3 signal deps. **No recognized join key** (see §3.3). |
| secop_budget_commitments | skc9-met7 | SECOP II - Compromisos Presupuestales | 5,866,588 | **Keep_context**. 1 signal dep. Join key: `id_contrato`. |
| secop_budget_items | cwhv-7fnp | SECOP II - Rubros Presupuestales | 5,891,602 | **Quarantined**. Join key: `id_contrato`. Needs quality fix. |

### 1.4 Identity / reference

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_suppliers | qmzu-gj57 | SECOP II - Proveedores Registrados | 1,569,704 | **Keep**. 5 signal deps. Join key: `nit`. |
| secop_sanctions | it5q-hg94 | SECOPII - Multas y Sanciones | ? | **Keep**. 1 signal dep used, 69 evidence refs. |

### 1.5 Location / context

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_additional_locations | wwhe-4sq8 | SECOP II - Ubicaciones Adicionales | 5,750,947 | **Review_or_drop**. 0 signal deps, 0 evidence refs. Loaded but unused. |
| secop_execution_locations | gra4-pcp2 | SECOP II - Ubicaciones ejecucion contratos | ? | **Keep_context**. 4 evidence refs. Join keys: `codigo_entidad`, `codigo_proveedor`, `id_contrato`. |
| secop_document_archives | dmgg-8hin | SECOP II - Archivos Descarga Desde 2025 | 18,663,743 | **Review_or_drop**. Loaded but not clearly used. Join key: `nit_entidad`. |
| secop_i_resource_origins | 3xwx-53wt | SECOP I - Origen de los Recursos | 5,188,217 | **Review_or_drop**. Loaded but not clearly used. Join key: `codigo_bpin`. |

### 1.6 Agreements

| source_id | dataset_id | Name | Rows | Registry Notes |
|---|---|---|---|---|
| secop_interadmin_agreements | s484-c9k3 | SECOP - Convenios Interadministrativos | 87,088 | **Keep_or_fix_deps**. Promoted but signal deps don't use it clearly. Join keys: `id_contrato`, `id_entidad`, `id_proceso`. |

---

## 2. SECOP Candidate Datasets (not yet in pipeline) — Proven Join Keys

These were detected by the triage as having recognized join keys and classified as candidates for ingestion.

### 2.1 ingest_priority (2+ core join keys, ≥1K rows)

| # | dataset_id | Name | Rows | Join Keys | Density | Assessment |
|---|---|---|---:|---|---|---|
| 1 | **3hdv-smhz** | TVEC - Compras por item | 200,000 | entity:`nit_entidad`, nit:`nit_proveedor` | 5/5, 5/5 | **Strong ingest.** Item-level procurement data from Tienda Virtual del Estado Colombiano. Links entities and suppliers via NIT. Low row count makes it tractable. Fills the "what was bought" signal gap. Additional columns: `orden_de_compra`, `fecha`, `entidad`, `provedor`, `item`, `price`, `qty`, `unidad_de_medida`, `line_total`, `cdp`. |
| 2 | **rgxm-mmea** | Tienda Virtual del Estado Colombiano - Consolidado | 162,919 | entity:`nit_entidad`/`id_entidad`, nit:`nit_proveedor` | 5/5, 5/5 | **Strong ingest.** Aggregated purchase orders from the government e-store. Links entities and suppliers. Additional columns include: `a_o`, `rama_de_la_entidad`, `orden_de_la_entidad`, `sector_de_la_entidad`, `entidad`, `solicitante`, `estado`, `solicitud`, `items`, `total`, `ciudad`, `actividad_economica_proveedor`. |
| 3 | **tauh-5jvn** | SECOP I - Proponentes | 117,781 | contract:`numero_contrato`, process:`id_proceso` | 5/5, 5/5 | **Strong ingest.** Maps which bidders/proponents participated in which processes. Key for analyzing competition and bidder networks. Additional columns: `tipo_doc_proponente`, `num_doc_proponente`, `proponente`, `calificacion`, `adjudicado`, `id_adjudicacion`, `fecha_publicacion_del_proceso`. |
| 4 | **s484-c9k3** | SECOP - Convenios Interadministrativos | 87,088 | contract:`id_contrato`, entity:`id_entidad`, process:`id_proceso` | 5/5, 5/5, 5/5 | **Already in registry** (keep_or_fix_deps). Inter-administrative agreements. Important for tracking cross-entity collusion and resource transfers. Additional columns: `numero_de_contrato`, `nombre_entidad`, `objeto_contractual`, `valor_con_adiciones`, `contratista`, `identificacion_contratista`, `fuente`, `origen_de_los_recursos`. |
| 5 | **4n4q-k399** | Multas y Sanciones SECOP I | 1,703 | entity:`nit_entidad`, nit:`documento_contratista` | 5/5, 5/5 | **Strong ingest.** Complements `it5q-hg94` (SECOP II sanctions). SECOP I-era fines/sanctions dataset. Very small. Additional columns: `nombre_entidad`, `nivel`, `orden`, `municipio`, `numero_de_resolucion`, `nombre_contratista`, `numero_de_contrato`, `valor_sancion`, `fecha_de_publicacion`, `fecha_de_firmeza`. |

### 2.2 ingest — already in registry (not yet loaded or enrichment_only)

| # | dataset_id | Name | Rows | Join Keys | Assessment |
|---|---||---:|---|---|
| 6 | **cb9c-h8sn** | SECOP II - Adiciones | 17,286,227 | contract:`id_contrato` | Already in registry (promoted). Contract value additions. |
| 7 | **p6dx-8zbt** | SECOP II - Procesos de Contratación | 8,411,769 | entity:`nit_entidad`/`codigo_entidad` | Already in registry (promoted). Process master data. |
| 8 | **4ex9-j3n8** | SECOP II - Contacto Entidades y Proveedores | 2,039,934 | entity:`codigo_entidad`/`nit_entidad` | **New candidate.** Rich entity/supplier directory: name, NIT, type, contact info, municipality, website, representative legal info, is_pyme. Very valuable for entity resolution and deduplication. **Recommend: ingest.** |
| 9 | **qmzu-gj57** | SECOP II - Proveedores Registrados | 1,569,704 | nit:`nit` | Already in registry (promoted, 5 signal deps). |
| 10 | **ps88-5e3v** | SECOP I - Archivos Descarga | 32,541,440 | entity:`codigo_entidad` | Already in registry (review_or_drop). Very large doc archive metadata. |
| 11 | **dmgg-8hin** | SECOP II - Archivos Descarga Desde 2025 | 18,663,743 | entity:`nit_entidad` | Already in registry (review_or_drop). |
| 12 | **u99c-7mfm** | SECOP II - Suspensiones de Contratos | 443,154 | contract:`id_contrato` | Already in registry (promoted). |
| 13 | **e2u2-swiw** | SECOP II - Modificaciones a Procesos | 24,744 | entity:`nit_entidad`/`codigo_entidad` | **New candidate.** Process-level modifications (changes to schedule, terms, etc.). Small dataset. Links via entity keys. **Recommend: ingest** for process mutation audit trail. |
| 14 | **b6m4-qgqv** | SECOP II - PAA - Encabezado | 52,517 | entity:`codigo_entidad` | **New candidate.** Annual Acquisition Plan headers per entity. Links entities to their planned procurement. **Recommend: ingest** for procurement planning analysis. |
| 15 | **36vw-pbq2** | SECOP I - Modificaciones a Procesos | 21,773 | process:`id_proceso` | **New candidate.** SECOP I-era process modifications. Small. Complements the SECOP II modifications dataset. **Recommend: ingest.** |
| 16 | **skc9-met7** | SECOP II - Compromisos Presupuestales | 5,866,588 | contract:`id_contrato` | Already in registry (keep_context). |

### 2.3 ingest_if_useful (join keys but <5K rows or unknown row count)

| # | dataset_id | Name | Rows | Join Keys | Assessment |
|---|---|---:|---|---|---|
| 17 | **9sue-ezhx** | SECOPII - Plan Anual De Adquisiciones Detalle | -1 (timeout) | entity:`codigo_entidad`/`nit_entidad` | **New candidate.** PAA line items with UNSPSC codes. 32 columns including entity keys, estimated values, modalities. Valuable for procurement planning analysis. ~52517 parent PAA records in `b6m4-qgqv`, so this detail table is likely 10x-100x. **Recommend: ingest** (re-probe row count first). Columns: `codigo_entidad`, `nit_entidad`, `valor_total_esperado`, `modalidad`, `categorias_unspsc`, `id_paa_encabezado`. |
| 18 | **gjp9-cutm** | SECOP II - Garantias | 0 | contract:`id_contrato` | **New candidate — BUT 0 rows.** Policy/guarantee info per contract. Currently empty (may have been cleared or restructured). **Recommend: skip** until confirmed to have data. 13 columns include: `aseguradora`, `numeropoliza`, `tipopoliza`, `subtipopoliza`, `valor`. |
| 19 | **hgi6-6wh3** | Proponentes por Proceso SECOP II | -1 (timeout) | entity:`nit_entidad`/`codigo_entidad`, nit:`nit_proveedor` | **New candidate.** Who bid on each process. 9 columns: `id_procedimiento`, `nit_entidad`, `codigo_entidad`, `proveedor`, `nit_proveedor`, `codigo_proveedor`. Very valuable for bidder network analysis. **Recommend: ingest** (re-probe row count first). |
| 20 | **prdx-nxyp** | SECOP I - PAA Encabezado | -1 (timeout) | divipola:`codigo_municipio`, entity:`codigo_entidad`/`nit_entidad` | **New candidate.** SECOP I-era Annual Acquisition Plan headers. 20 columns include PAA totals, entity info, geographic data. **Recommend: ingest** (re-probe row count first). |

### 2.4 context_enrichment (BPIN/divipola join)

| # | dataset_id | Name | Rows | Join Keys | Assessment |
|---|---|---:|---|---|---|
| 21 | **f789-7hwg** | SECOP I - Procesos de Compra Pública | 6,357,442 | bpin:`codigo_bpin` | **New candidate.** SECOP I process-level data w/ BPIN codes. 7M+ rows. Complements `qddk-cgux` (SECOP I Historical). **Recommend: context_enrichment** — useful for BPIN-project linking. |
| 22 | **3xwx-53wt** | SECOP I - Origen de los Recursos | 5,188,217 | bpin:`codigo_bpin` | Already in registry (review_or_drop). |
| 23 | **cf9k-55fw** | DNP-proyectos_datos_basicos | 531,220 | bpin:`bpin` | **New candidate.** DNP project master data with BPIN codes. Links BPIN to project names, status, budgets, responsible entities. **Recommend: context_enrichment** for BPIN-to-project mapping. |

---

## 3. SECOP Datasets with No Recognized Join Keys (manual column review)

These are SECOP datasets where the automated triage found no standard join key column names. I manually reviewed their schemas to determine if they contain latent join keys under different column naming conventions.

### 3.1 mfmm-jqmq — SECOP II - Ejecución Contratos (4,394,443 rows)

**Columns:** `identificadorcontrato | tipoejecucion | nombreplan | fechadeentregaesperada | porcentajedeavanceesperado | fechadeentregareal | porcentaje_de_avance_real | estado_del_contrato | referencia_de_articulos | descripci_n | unidad | cantidad_adjudicada | cantidad_planeada | cantidadrecibida | cantidadporrecibir | fechacreacion`

**Analysis:** The `identificadorcontrato` column is a camelCase variant of `id_contrato`. This is a **latent contract join key** that the triage normalization missed because it lacks underscores. The dataset tracks contract execution milestones — deliverables, quantities, completion percentages. Very valuable for contract performance analysis.

**Recommendation: INGEST.** Maps `identificadorcontrato` → `id_contrato`. Needs custom normalization rule.

### 3.2 7fix-nd37 — SECOP I - Adiciones (1,491,905 rows)

**Columns:** `id_adjudicacion | adicion_en_valor | adicion_en_dias | adicion_en_meses | fecha_firma`

**Analysis:** This is the SECOP I counterpart of the SECOP II Adiciones (`cb9c-h8sn`). Only has `id_adjudicacion`, not `id_contrato`. The `id_adjudicacion` links to SECOP I awarded processes. The SECOP I Historical Process dataset (`qddk-cgux`) has `id_adjudicacion` as well, so this is joinable via that key — but `id_adjudicacion` is not one of our 6 standard join key classes.

**Recommendation: DEFER.** Needs `id_adjudicacion` → contract/process join through SECOP I Historical. Useful but requires two-hop joining. Potential ingest if SECOP I Historical is enriched to expose `id_adjudicacion` as a linking key.

### 3.3 wi7w-2nvm — SECOP II - Ofertas Por Proceso (row count: -1/timed out)

**Columns:** `fecha_de_registro | referencia_de_la_oferta | identificador_de_la_oferta | valor_de_la_oferta | entidad_compradora | nit_entidad_compradora | moneda | descripcion_del_procedimiento | referencia_del_proceso | id_del_proceso_de_compra | modalidad | invitacion_directa | nombre_proveedor | nit_del_proveedor | c_digo_entidad | c_digo_proveedor`

**Analysis:** This has **latent join keys**: `nit_entidad_compradora` (entity NIT), `nit_del_proveedor` (supplier NIT), `id_del_proceso_de_compra` (process ID), `c_digo_entidad` (entity code), `c_digo_proveedor` (supplier code). The triage missed these because the column names diverge from standard patterns — `nit_entidad_compradora` vs. `nit_entidad`, `id_del_proceso_de_compra` vs. `id_proceso`. This is **extremely valuable** — it shows who bid on each process, at what price, winning/losing bidders.

**Recommendation: INGEST_PRIORITY.** Maps: `nit_entidad_compradora` → entity NIT, `nit_del_proveedor` → supplier NIT, `id_del_proceso_de_compra` → process ID, `c_digo_entidad` → entity code, `c_digo_proveedor` → supplier code. Re-probe row count (likely millions). Critical for bid-rigging and competition analysis.

### 3.4 ceth-n4bn — Grupos de Proveedores - SECOP II (690,885 rows)

**Columns:** `codigo_grupo | nombre_grupo | nit_grupo | es_entidad | es_proveedor | esta_activo | fecha_creaci_n_grupo | ... | codigo_participante | nombre_participante | nit_participante | tipo_empresa_participante | ubicaci_n_participante | fecha_creaci_n_participante | participacion | es_lider_del_grupo`

**Analysis:** Contains `nit_grupo` and `nit_participante` — these are **latent NIT join keys**. The triage missed them because they use `_grupo` and `_participante` suffixes instead of the bare `_proveedor` or `_entidad` patterns. This dataset maps consortiums/grupos (joint bidding entities) to their member suppliers. Very valuable for detecting collusion — who teams up with whom.

**Recommendation: INGEST.** Maps: `nit_grupo` → NIT (supplier group NIT), `nit_participante` → NIT (individual member NIT). Key for collusion network analysis.

### 3.5 azeg-sgqg — SECOP I - PAA Detalle (586,965 rows)

**Columns:** `id | codigo_unspsc | descripcio_item | fecha_inicio | duracion_estimada | modalidad | fuente_de_recursos | valor_estimado | valor_estimado_vig_actual | requiere_vigencias_futuras | contacto_responsable_adquisicion | idpaa`

**Analysis:** No standard join key columns. Has `idpaa` which links to the PAA Encabezado (`prdx-nxyp`) where entity/bpin keys live. No direct entity, contract, or NIT columns. This is a detail/child table that can only be joined through the parent PAA header.

**Recommendation: DEFER.** Useful only as enrichment for PAA Encabezado. Ingest `prdx-nxyp` and `9sue-ezhx` first, then consider joining this detail data.

### 3.6 u5b4-ae3s — SECOP I - Modificaciones a Adjudicaciones (813,799 rows)

**Columns:** `id_adjudicacion | id_modificacion | fecha_modificacion | campo_modificado | valor_anterior | valor_nuevo | justificacion_cambio`

**Analysis:** Analogous to `36vw-pbq2` (SECOP I modifications to processes) but for adjudications. Has `id_adjudicacion` which is the same SECOP I adjudication key. Also has `id_modificacion`, `campo_modificado`, `valor_anterior`, `valor_nuevo` — this is an audit trail of field-level changes to adjudications. Very valuable for detecting suspicious modifications.

**Recommendation: INGEST_IF_USEFUL.** Requires `id_adjudicacion` linking through SECOP I Historical. Same two-hop issue as Adiciones. The audit-trail format (`campo_modificado`/`valor_anterior`/`valor_nuevo`) is extremely powerful for corruption detection.

---

## 4. TVEC Datasets (Tienda Virtual del Estado Colombiano)

### 4.1 usqp-5nsn — TVEC - Items (1,367,593 rows)

**Join key:** entity:`id_entidad` (5/5 density). **Ingest class: ingest.**

This is the item-level detail of what was actually purchased. Links to `3hdv-smhz` (TVEC - Compras por item) and `rgxm-mmea` (TVEC Consolidado).

**Recommendation: INGEST.** Provides item-level granularity for TVEC purchases.

### 4.2 3hdv-smhz — TVEC - Compras por item (200,000 rows)

**Join keys:** entity:`nit_entidad`, nit:`nit_proveedor`. **Ingest class: ingest_priority.**

Already reviewed in §2.1 #1 above.

### 4.3 rgxm-mmea — Tienda Virtual del Estado Colombiano - Consolidado (162,919 rows)

**Join keys:** entity:`nit_entidad`/`id_entidad`, nit:`nit_proveedor`. **Ingest class: ingest_priority.**

Already reviewed in §2.1 #2 above.

---

## 5. Non-SECOP Supervised Entity Datasets (Financial Sector)

These are Superfinanciera / NIIF datasets providing financial information on supervised entities. They link via `codigo_entidad` or `nit` to the co-acc entity graph.

| # | dataset_id | Name | Rows | Join Key | Assessment |
|---|---|---:|---|---|---|
| 24 | mxk5-ce6w | Información financiera con fines de supervisión – CUIF por moneda Entidades vigiladas | 19,344,191 | entity:`codigo_entidad` | **ingest.** Massive financial dataset. Very large row count — needs lake-first approach. |
| 25 | pfdp-zks5 | Estados Financieros NIIF- Estado de Situación Financiera | 17,851,220 | nit:`nit` | **ingest.** Balance sheet data per NIT. Very large. |
| 26 | 6hqw-m3dm | Estados Financieros NIIF- Carátula | 8,685,453 | nit:`nit` | **ingest.** NIIF report headers — maps NIT to taxonomy/period. |
| 27 | prwj-nzxa | Estados Financieros NIIF- Estado de Resultado Integral | 7,320,752 | nit:`nit` | **ingest.** Income statement data. |
| 28 | ctcp-462n | Estados Financieros NIIF- Estado de Flujo Efectivo | 5,769,293 | nit:`nit` | **ingest.** Cash flow data. |
| 29 | y3gh-x5g7 | Estados Financieros NIIF- Otro Resultado Integral | 1,707,822 | nit:`nit` | **ingest.** Other comprehensive income. |
| 30 | r3d5-pipz | Balance de entidades publicas vigiladas por la SFC | 886,335 | entity:`codigo_entidad` | **ingest.** SFC-supervised entity balance data. |
| 31 | xyy7-rn7p | Quejas interpuestas por los consumidores financieros | 1,182,619 | entity:`codigo_entidad` | **ingest.** Consumer complaints against financial entities — direct corruption/irregularity signal. |
| 32 | dd55-74ss | SUJETOS OBLIGADOS | 161,771 | nit:`nit` | **ingest.** Registry of entities obligated to report — useful for coverage validation. |
| 33 | e967-4a8r | Información estadística y financiera por ramos de seguros F-290 | 236,379 | entity:`codigo_entidad` | **ingest.** Insurance sector stats per entity. |
| 34 | rvii-eis8 | Distribución de cartera por producto | 107,721 | entity:`codigo_entidad` | **ingest.** Loan portfolio distribution per entity. |
| 35 | uawh-cjvi | Fondo de Pensiones Obligatorias y Cesantías C.P L.P | 93,963 | entity:`codigo_entidad` | **ingest.** Pension fund data per entity. |
| 36 | qhpu-8ixx | Rentabilidades de los Fondos de Inversión Colectiva (FIC) | 2,762,002 | entity:`codigo_entidad` | **ingest.** FIC performance per entity. |
| 37 | sr9n-792w | Entidades vigiladas por la Superfinanciera | 452 | entity:`cod_entidad` | **ingest_if_useful.** Small reference table of supervised entities. `cod_entidad` (not `codigo_entidad`). |
| 38 | 7jfv-7spn | Montos y número de créditos aprobados o desembolsados | 49,829 | entity:`codigo_entidad` | **ingest.** Credit data per entity. |
| 39 | tic6-rbue | Estados financieros de entidades solidarias desde 2017 | -1 | entity:`codigo_entidad`, nit:`nit` | **ingest_if_useful.** Solidary sector financials. Re-probe needed. |

---

## 6. Sanctions Datasets

| # | dataset_id | Name | Rows | Join Keys | Assessment |
|---|---|---:|---|---|---|
| 40 | 4n4q-k399 | Multas y Sanciones SECOP I | 1,703 | entity:`nit_entidad`, nit:`documento_contratista` | **ingest_priority.** Already reviewed in §2.1. |
| 41 | it5q-hg94 | SECOPII - Multas y Sanciones | ? | (already in registry) | Already in registry. |

---

## 7. Priority Ingest Recommendations

### Tier 1 — Highest Priority (immediate ingestion, clear join keys, high signal)

| Priority | dataset_id | Name | Join Key Mapping | Why |
|---:|---|---|---|---|
| 1 | **wi7w-2nvm** | SECOP II - Ofertas Por Proceso | `nit_entidad_compradora`→nit, `id_del_proceso_de_compra`→process, `c_digo_entidad`→entity, `nit_del_proveedor`→nit, `c_digo_proveedor`→entity | **Already in registry but mis-parsed.** Critical for bid-rigging detection. 5 latent join keys. |
| 2 | **mfmm-jqmq** | SECOP II - Ejecución Contratos | `identificadorcontrato`→contract | **Already in registry but mis-parsed.** Contract execution tracking. 4.4M rows. |
| 3 | **ceth-n4bn** | Grupos de Proveedores - SECOP II | `nit_grupo`→nit, `nit_participante`→nit | Consortium/group mapping for collusion detection. |
| 4 | **hgi6-6wh3** | Proponentes por Proceso SECOP II | `nit_entidad`→entity, `codigo_entidad`→entity, `nit_proveedor`→nit | Who bid on what. Bidder network analysis. |
| 5 | **tauh-5jvn** | SECOP I - Proponentes | `numero_contrato`→contract, `id_proceso`→process | SECOP I bidder data. |
| 6 | **3hdv-smhz** | TVEC - Compras por item | `nit_entidad`→entity, `nit_proveedor`→nit | Item-level gov procurement. |
| 7 | **rgxm-mmea** | Tienda Virtual del Estado Consolidado | `nit_entidad`→entity, `id_entidad`→entity, `nit_proveedor`→nit | Aggregated gov e-store purchases. |
| 8 | **4n4q-k399** | Multas y Sanciones SECOP I | `nit_entidad`→entity, `documento_contratista`→nit | SECOP I sanctions complement. |

### Tier 2 — High Priority (clear value, standard join keys)

| Priority | dataset_id | Name | Join Key | Why |
|---:|---|---|---|---|
| 9 | 4ex9-j3n8 | SECOP II - Contacto Entidades y Proveedores | `codigo_entidad`, `nit_entidad` | Entity/supplier master directory. |
| 10 | e2u2-swiw | SECOP II - Modificaciones a Procesos | `nit_entidad`, `codigo_entidad` | Process modification audit trail. |
| 11 | b6m4-qgqv | SECOP II - PAA - Encabezado | `codigo_entidad` | Annual procurement plan headers per entity. |
| 12 | 36vw-pbq2 | SECOP I - Modificaciones a Procesos | `id_proceso` | Process modification audit trail (SECOP I). |
| 13 | 9sue-ezhx | SECOPII - PAA Detalle | `codigo_entidad`, `nit_entidad` | Procurement plan items with UNSPSC codes. |
| 14 | prdx-nxyp | SECOP I - PAA Encabezado | `codigo_entidad`, `nit_entidad`, `codigo_municipio` | SECOP I PAA data with geographic context. |

### Tier 3 — Useful Context/Enrichment

| Priority | dataset_id | Name | Join Key | Why |
|---:|---|---|---|---|
| 15 | f789-7hwg | SECOP I - Procesos de Compra Pública | `codigo_bpin` | BPIN-linked processes. |
| 16 | cf9k-55fw | DNP-proyectos_datos_basicos | `bpin` | DNP project master data via BPIN. |
| 17 | usqp-5nsn | TVEC - Items | `id_entidad` | Item-level TVEC detail. |
| 18 | xyy7-rn7p | Quejas consumidores financieros | `codigo_entidad` | Consumer complaints = direct irregularity signal. |

### Tier 4 — Defer (requires two-hop joins or low priority)

| Priority | dataset_id | Name | Why Defer |
|---:|---|---|---|
| 19 | 7fix-nd37 | SECOP I - Adiciones | Requires `id_adjudicacion` → SECOP I Historical lookup. |
| 20 | u5b4-ae3s | SECOP I - Modificaciones a Adjudicaciones | Same `id_adjudicacion` two-hop issue. Audit trail format is powerful though. |
| 21 | azeg-sgqg | SECOP I - PAA Detalle | Only joinable through PAA Encabezado parent. |
| 22 | gjp9-cutm | SECOP II - Garantias | Currently 0 rows. Skip until data available. |

---

## 8. Action Items

1. **Fix join key normalization** for `identificadorcontrato` (SECOP II Ejecución) and `nit_entidad_compradora`/`id_del_proceso_de_compra`/`nit_del_proveedor`/`c_digo_entidad`/`c_digo_proveedor` (SECOP II Ofertas) and `nit_grupo`/`nit_participante` (Proveedores Grupo). Add these patterns to `coacc_etl.source_qualification`.
2. **Re-probe row counts** for `9sue-ezhx`, `hgi6-6wh3`, `prdx-nxyp`, and `wi7w-2nvm` (all failed/timed out).
3. **Wire `wi7w-2nvm` (SECOP II Ofertas)** and `mfmm-jqmq` (SECOP II Ejecución) with correct join key aliases since they're already in the registry but mis-parsed.
4. **Consider demoting or removing** `wwhe-4sq8` (SECOP II Ubicaciones Adicionales) and `dmgg-8hin` (SECOP II Archivos Descarga) from the pipeline — both are in registry but marked "review_or_drop" with 0 signal deps and 0 evidence refs.
5. **Un-quarantine `cwhv-7fnp`** (SECOP II Rubros Presupuestales) once join quality is verified — it has `id_contrato` at 5/5 density.

---

## 9. Missed Datasets — Not in Previous Review

These datasets were not caught by the automated triage because their column names use non-standard patterns. Manual schema probing and density checks confirm they have strong join keys.

### 9.1 f789-7hwg — SECOP I - Procesos de Compra Pública (6,357,442 rows)

Previous review listed this as `context_enrichment` (only BPIN detected). **This was severely under-classified.**

**Latent join keys (confirmed 100% density):**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `nit_de_la_entidad` | **entity NIT** | 6,357,497 | ~100% |
| `c_digo_de_la_entidad` | **entity code** | 6,354,655 | ~99.96% |
| `identificacion_del_contratista` | **supplier NIT/ID** | 6,357,497 | ~100% |
| `codigo_bpin` | BPIN | 6,357,497 | ~100% |
| `numero_de_proceso` | process ref | 6,357,497 | ~100% |

**This has 5 join keys at near-100% density.** It's the SECOP I equivalent of `jbjy-vk9h` (SECOP II Contratos). 79 columns including full contractor identity, process details, value data, modification tracking, post-conflict flags, etc. It was misclassified because the triage didn't recognize `nit_de_la_entidad`, `c_digo_de_la_entidad`, or `identificacion_del_contratista` as join key patterns (underscore-based `c_digo` instead of `codigo`, `identificacion_del_contratista` instead of `documento_proveedor`).

**Reclassification: ingest_priority.** This is a **core** dataset equivalent to SECOP II Contratos for the pre-2015 era.

### 9.2 h7zv-k39x — Universo de entidades (6,404 rows)

**In triage `no_join` class.**

**Latent join keys (confirmed 100% density):**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `dm_institucion_cod_institucion` | **entity code** | 6,404 | 100% |
| `ccb_nit_inst` | **NIT** | 6,404 | 100% (many are "NULL" strings though) |
| `idmunicipio` | **DIVIPOLA municipality** | 6,404 | 100% |
| `iddepartamento` | **DIVIPOLA department** | 6,404 | 100% |

This is the **master directory of all Colombian public entities** — name, NIT, sector, order, classification, nature, location. Extremely valuable for entity resolution. The `ccb_nit_inst` field contains many "NULL" string values (3rd-party data), so NIT density is lower than it appears — but entity code is solid.

**Reclassification: ingest.** Key entity reference dataset.

### 9.3 5phs-yqfw — Información de Gastos del Presupuesto General de la Nación (581,147 rows)

**In triage `large_no_join` class.**

**Latent join keys:**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `codigoentidad` | **entity code** | 581,147 | 100% |

Budget execution data for the national government — appropriations, additions, reductions, commitments, obligations, payments per entity per month. 581K rows of granular spending data. `codigoentidad` follows the `XX-YY-ZZ` format (sector-entity-unit).

**Reclassification: ingest.** Entity-level budget execution is a direct corruption signal (deviations from budget, anomalous spending).

### 9.4 bpij-5vy9 — Ejecución Presupuestal del PGN por Rubro Presupuestal (3,645 rows)

**In triage `no_join` class.**

**Latent join keys:**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `codigoentidad` | **entity code** | 3,645 | 100% |

Same entity code format as `5phs-yqfw`. More detailed breakdown by budget line item (5 levels of rubro). Smaller row count = summary/aggregated level. Complements the gastos dataset.

**Reclassification: ingest_if_useful.** Redundant with `5phs-yqfw` but provides rubro breakdown.

### 9.5 xjxk-qhsc — Ejecución Presupuestal del PGN (554 rows)

Already in `ingest_if_useful` with `codigo_entidad` detected. No new latent keys found. Confirmed.

### 9.6 br9a-gygu — Ejecución Financiera de Regalías (20,856 rows)

**In triage `no_join` class.**

**Latent join keys:**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `codigobpin` | **BPIN** | 20,856 | 100% |
| `codigodaneentidad` | **DIVIPOLA entity** | 20,856 | 100% |

Royalties execution data per project per entity. Links through BPIN to project master data and through DANE entity code to territorial entities.

**Reclassification: ingest.** Direct corruption signal — royalties are a known high-risk fund flow.

### 9.7 mzgh-shtp — DNP-ProyectosSGR (35,006 rows)

**In triage `large_no_join` class, already in registry (keep, already_used).**

**Latent join keys:**
| Column | Join class | Non-null count | Density |
|---|---|---:|---|
| `codigobpin` | **BPIN** | 35,006 | 100% |

SGR (Sistema General de Regalías) project data — execution status, values, interventor, OCAD, department. Already in the current registry but mis-parsed as `no_join` by the triage.

**Action: Fix join key detection for `codigobpin`** → should map to BPIN class.

### 9.8 fs36-azrv — Registro de Sanciones Contadores (139 rows)

**In triage `no_join` class.**

**Latent join keys:**
| Column | Join class | Assessment |
|---|---|---|
| `c_dula` | **NIT/ID** | Person-level ID of sanctioned accountants |

Small but relevant — sanctioned accountants by the Junta Central de Contadores. `c_dula` is a person ID number (not NIT), so it may not match our entity-focused join keys. Still, it's a sanctions dataset.

**Reclassification: ingest_if_useful.** Low row count, person-level (not entity-level) sanctions.

### 9.9 kg2d-yfyg — Listado de Entidades del Sector Solidario (121,610 rows)

**Already in `ingest` class with `nit:nit`.**

However, it also has `codentidad` which is an entity code. This was already correctly classified. No change needed.

### 9.10 cf9k-55fw — DNP-proyectos_datos_basicos (531,220 rows)

**Already in `context_enrichment` with `bpin:bpin`.**

Also has `codigoentidadresponsable` which is an entity code. Sample data shows it uses a simple numeric code, not the `XX-YY-ZZ` format. May need custom mapping.

**Reclassification: ingest** (promoted from context_enrichment). Provides project-level entity responsibility data.

---

## 10. Revised Top Priority List (including latent-key discoveries)

### Tier 1 — Core (immediate)

| # | dataset_id | Name | Proven Join Keys | Why |
|---|---|---|---|---|
| 1 | **f789-7hwg** | SECOP I - Procesos de Compra Pública | entity NIT, entity code, supplier ID, BPIN, process ref | **THE missed dataset.** 6.4M rows, 79 cols, 5 join keys at 100%. Equivalent to SECOP II Contratos for pre-2015 data. |
| 2 | **wi7w-2nvm** | SECOP II - Ofertas Por Proceso | entity NIT, process, supplier NIT, entity code, supplier code | Already in registry but mis-parsed. Bid-rigging detection. |
| 3 | **mfmm-jqmq** | SECOP II - Ejecución Contratos | contract (`identificadorcontrato`) | Already in registry but mis-parsed. Execution tracking. |
| 4 | **ceth-n4bn** | Grupos de Proveedores - SECOP II | NIT (grupo), NIT (participante) | Consortium/collusion mapping. |
| 5 | **hgi6-6wh3** | Proponentes por Proceso SECOP II | entity NIT, entity code, supplier NIT | Bidder network analysis. |
| 6 | **tauh-5jvn** | SECOP I - Proponentes | contract, process | SECOP I bidder data. |
| 7 | **3hdv-smhz** | TVEC - Compras por item | entity NIT, supplier NIT | Item-level gov purchases. |
| 8 | **rgxm-mmea** | TVEC Consolidado | entity NIT, entity ID, supplier NIT | Aggregated gov e-store. |
| 9 | **4n4q-k399** | Multas y Sanciones SECOP I | entity NIT, supplier NIT | SECOP I sanctions. |
| 10 | **h7zv-k39x** | Universo de entidades | entity code, NIT, DIVIPOLA | Master entity directory. |

### Tier 2 — High Value (strong signal + standard/latent keys)

| # | dataset_id | Name | Proven Join Keys | Why |
|---|---|---|---|---|
| 11 | **5phs-yqfw** | Información de Gastos PGN | entity code | Budget execution — direct corruption signal. |
| 12 | **br9a-gygu** | Ejecución Financiera de Regalías | BPIN, DIVIPOLA entity | Royalties execution — high-risk fund flow. |
| 13 | **cf9k-55fw** | DNP-proyectos_datos_basicos | BPIN, entity code | Project master data via BPIN. |
| 14 | **mzgh-shtp** | DNP-ProyectosSGR | BPIN | SGR project execution. |
| 15 | **4ex9-j3n8** | SECOP II - Contacto Entidades y Proveedores | entity code, entity NIT | Entity/supplier directory. |
| 16 | **e2u2-swiw** | SECOP II - Modificaciones a Procesos | entity NIT, entity code | Process modification audit. |
| 17 | **b6m4-qgqv** | SECOP II - PAA - Encabezado | entity code | Procurement plan headers. |
| 18 | **36vw-pbq2** | SECOP I - Modificaciones a Procesos | process ID | Process modification audit (SECOP I). |
| 19 | **9sue-ezhx** | SECOPII - PAA Detalle | entity code, entity NIT | PAA line items. |
| 20 | **prdx-nxyp** | SECOP I - PAA Encabezado | entity code, entity NIT, DIVIPOLA | SECOP I PAA data. |
| 21 | **xyy7-rn7p** | Quejas consumidores financieros | entity code | Consumer complaints = irregularity signal. |

### Tier 3 — Context/Enrichment

| # | dataset_id | Name | Join Key | Why |
|---|---|---|---|---|
| 22 | usqp-5nsn | TVEC - Items | entity ID | Item-level TVEC detail. |
| 23 | bpij-5vy9 | Ejecución PGN por Rubro | entity code | Budget rubro detail. |
| 24 | 3xwx-53wt | SECOP I - Origen de los Recursos | BPIN | Resource origins (already in registry). |
| 25 | d9na-abhe | SECOP II - BPIN por Proceso | BPIN, process, contract | Process-BPIN mapping (already in registry). |
| 26 | dd55-74ss | SUJETOS OBLIGADOS | NIT | Registry of reporting-obligated entities. |
| 27 | fs36-azrv | Registro de Sanciones Contadores | cedula | Sanctioned accountants. |

---

## 11. Updated Action Items

6. **Fix `f789-7hwg`** (SECOP I Procesos) classification from `context_enrichment` to `ingest_priority`. Add normalization rules for: `nit_de_la_entidad` → entity NIT, `c_digo_de_la_entidad` → entity code, `identificacion_del_contratista` → supplier NIT, `numero_de_proceso` → process ref.
7. **Fix `h7zv-k39x`** (Universo de entidades) from `no_join` to `ingest`. Add normalization for: `dm_institucion_cod_institucion` → entity code, `ccb_nit_inst` → NIT (note "NULL" strings), `idmunicipio`/`iddepartamento` → DIVIPOLA.
8. **Fix `5phs-yqfw`** (Gastos PGN) from `large_no_join` to `ingest`. Add `codigoentidad` → entity code (camelCase, no underscore).
9. **Fix `br9a-gygu`** (Ejecución Regalías) from `no_join` to `ingest`. Add `codigobpin` → BPIN, `codigodaneentidad` → DIVIPOLA entity.
10. **Fix `mzgh-shtp`** (DNP-ProyectosSGR) — already in registry but triage missed `codigobpin` → BPIN. Add normalization.
11. **Fix `cf9k-55fw`** (DNP proyectos) — add `codigoentidadresponsable` → entity code. Promote from `context_enrichment` to `ingest`.

---

## 12. Complete No-Join Dataset Review (Batches 1-6)

All 87 no-join datasets with recommendation ≥ candidate were manually probed for latent join keys. Results below.

### 12.1 NEW FINDINGS — Datasets with discovered latent join keys

| # | dataset_id | Name | Rows | Latent Join Keys | Classification |
|---|---|---|---:|---|---|
| 1 | **h2yr-zfb2** | Subsidios De Vivienda Asignados | 85,973 | `c_digo_divipola_departamento`, `c_digo_divipola_municipio` → **DIVIPOLA** | **ingest_if_useful** — Housing subsidies by municipality. Links to territorial entities via DIVIPOLA. |
| 2 | **s87b-tjcc** | Estadísticas Solicitudes Restitución Municipios | 1,042 | `codigodanemunicipiopredio` → **DIVIPOLA municipality** | **ingest_if_useful** — Land restitution claims by municipality. |
| 3 | **acs4-3wgp** | Detección de Cultivos de Coca (hectáreas) | 319 | `coddepto`, `codmpio` → **DIVIPOLA** | **context_enrichment** — Coca cultivation by municipality. Useful for corruption risk scoring of territories. |
| 4 | **pqdu-ej7f** | Predios Beneficiarios PIDAR | 6,031 | geographic columns (departamento, municipio, lat/long) | **context_enrichment** — agricultural land beneficiary data. No standard entity/NIT key but has geography. |
| 5 | **5wck-szir** | MEN_MATRICULA_ESTADISTICA_ES | 390,903 | `c_digo_de_la_instituci_n`, `c_digo_del_departamento_ies`, `c_digo_del_municipio_ies`, `c_digo_del_departamento_programa`, `c_digo_del_municipio_programa` → **DIVIPOLA** | **already in registry** — has DIVIPOLA codes. Could link institutions to municipalities. |
| 6 | **5n52-6rih** | Población Afiliada | 105 | `id_ccf`, `ccf` → **entity name** (Caja de Compensación) | **context_if_useful** — Small reference table; links CCF to population coverage. |
| 7 | **tnbg-fy2n** | Producción de Bienes Nacionales | 10,840 | `productor_nit` → **NIT** | **ingest_if_useful** — Producer NIT links industrial producers. |
| 8 | **cmgp-8z8t** / **2zn7-bez3** | Entidades Acreditadas | 133-140 | `n_mero_nit` → **NIT** | **ingest_if_useful** — Accredited entities by NIT. Multiple near-identical copies. |
| 9 | **sasi-u68b** | Entidades Acreditadas Oct 2023 | 127 | `numero_nit` → **NIT**, `n_mero_c_dula_representante` → person ID | **ingest_if_useful** — Richer than other Acreditadas. Has entity NIT + representative cedula. |
| 10 | **3y4s-dmxy** | MEN_INDICADORES_PRIMERA_INFANCIA | 10,107 | `cod_dane_depto`, `cod_dane_mun` → **DIVIPOLA** | **context_if_useful** — Child wellbeing indicators by municipality. |

### 12.2 CONFIRMED NO JOIN KEYS — Datasets with no entity/NIT/DIVIPOLA/BPIN keys

These have no recognizable join keys. Their columns are purely descriptive, aggregate, or use non-standard identifiers that don't map to our join key classes.

**Batch 1 — Justice/Legal (no keys):**
- `fuyf-sb4r` (Procesos en Casas de Justicia) — 4 cols: `idsolicitud`, `nomentidad`, `nomcasajusticia`, `fechasolicitud`. No entity code, no NIT, no DIVIPOLA.
- `yh8b-89bi` (Caracterización Personas Casas de Justicia) — 7 cols: demographic only. No keys.
- `fiev-nid6` (Lista de normas SUIN-Juriscol) — 8 cols: `tipo`, `n_mero`, `a_o`, `sector`, `entidad` (text, not code). No keys.
- `xqsb-y246` (USPEC PAA) — 18 cols: no entity/NIT code. Only `location` as text.
- `btz9-gir8` (Solicitudes de Arbitraje) — `departamento`/`municipio` as text, no DIVIPOLA codes.
- `xm4c-2rks` (Solicitudes de conciliación) — has `sic_codigo_dane` → **DIVIPOLA municipality**. **NEW FINDING.**
- `msu7-rjqd` (Solicitudes de insolvencia) — `municipio`/`departamento` as text only. No codes.
- `3fbf-wags` (Notificaciones Superfinanciera) — `nombre_del_notificado` (text name, no NIT/code). No keys.
- `d76u-8x6w` (Postulados situación penitenciaria) — `municipio`/`departamento` text only. No codes.
- `s34r-vs6z` (Actos administrativos Sistemas Locales Justicia) — text only. No codes.
- `wavk-2hmm`, `s44d-v5fw`, `us2n-5jaf`, `gkbc-gw7x` (Demobilizations) — all text `departamento`/`indicador`, no codes.
- `4bku-d9az` (Actuaciones judiciales Corte Constitucional) — `n_mero_de_proceso` (court case number, not our process ID). No keys.
- `kfcm-k5vw` (Seguimiento actividades MinJusticia) — all activity tracking, no entity/NIT.

**Batch 2 — Documentary inventories (no keys):**
- `dwbg-geen`, `qr2y-27z8` (Inventarios documentales) — archive inventory. No entity codes.
- `pzu3-75kc` (FOGACOOP activos) — info registry metadata. No keys.
- `mqxk-srbz` (Control Sustancias/Productos Químicos) — aggregate counts, no entity keys.
- `36ay-utxu` (UGPP Tasa de Evasión) — aggregate: `ano`, `obligacion`, `billones`, `porcentaje`. No entity.

**Batch 3 — Financial (no keys beyond already-found):**
- `xbk8-edux` (Reporte movilización Intermediario Financiero) — `nombre_intermediario` (text). No entity code.
- `f4a5-ab9q` (Seguimiento Ejecución Presupuestal Sector Justicia) — `entidad` and `descripci_n` (text). No entity code/NIT.
- `egce-jd6s` (Directorio funcionarios conciliar) — `nombre_entidad`, `departamento`, `ciudad` (text). No codes.
- `hjfm-ynaz` (Conciliadores por Municipio) — `departamento`/`municipio` text. No DIVIPOLA codes.
- `xfsi-rqje` (Afiliados Colpensiones) — text `departamento`/`municipio`. No codes.
- `hdnf-a76p` (Pensionados Colpensiones) — text. No codes.
- `jvtd-3dgy` (Pensionados por entidad) — `nombre_entidad_administradora` (text). No code column.
- `khhm-wccm` (Reporte intermediarios RUI) — `departamento` text only. No codes.

**Batch 4 — DIVIPOLA found in some, rest no keys:**
- `rqjn-d9f5` (Censo Nacional de Archivos) — `entidad_nombre_raz_n_social` (text). No NIT/code.
- `6hcf-xdqm` (Licencias Cannabis) — `empresa` (text name). No NIT.
- `9m2f-pdxx` (Cannabis stats) — all aggregate counts. No keys.
- `cm2t-qreq` (Encuesta seguridad vial) — `ik_divipola` → **DIVIPOLA**. **NEW FINDING.** But 402 columns of survey data.
- `4cym-4b76` (Directorio Consultorios Jurídicos) — text `ciudad`/`departamento`. No codes.
- `7p9a-zd9k` (Directorio Centros Conciliación) — text. No codes.
- `bmcc-69wd` (Directorio Casas de Justicia) — text `departamento`/`municipio`. No codes.
- `58aq-7nep` (Población Privada Libertad por Rango) — `establecimiento` (text name). No entity code.
- `mjt9-2zwc` (Población Privada Libertad por Nivel) — same. No keys.
- `yix6-7yeh` (Población NINI) — `dominio_geografico` (text). No codes.
- `dr5c-eewa` (Mortalidad en vias) — 0 columns (empty). No data.
- `fd7n-f4jp` (USPEC Alimentación PPL) — `regional`, `departamento`, `municipio` (text). No codes.
- `v3rx-q7t3` (Densidad Cultivos Coca - gridded) — `grilla1` (grid cell), `the_geom`. Geographic only.
- `kccg-dij4` (Directorio entidades conciliación/insolvencia) — text. No codes.

**Batch 5 — Education (DIVIPOLA codes found):**
- `upr9-nkiz` (MEN Programas Educación Superior) — `codigoinstitucion`, `codigodepartinstitucion`, `codigomunicipioinstitucion`, `codigodepartprograma`, `codigomunicipioprograma` → **DIVIPOLA**. **NEW FINDING.** Also has SNIES program codes.
- `y9ga-zwzy` (MEN Estadísticas Matrícula por Municipios) — `c_digo_deldepartamento`, `c_digo_delmunicipio` → **DIVIPOLA**. **NEW FINDING.**
- `nudc-7mev` (MEN Estadísticas por Municipio) — `c_digo_municipio`, `c_digo_departamento`, `c_digo_etc` → **DIVIPOLA**. **NEW FINDING.**
- `ji8i-4anb` (MEN Estadísticas por Departamento) — `c_digo_departamento` → **DIVIPOLA dept**. **NEW FINDING.**
- `4hrb-y62g` (MEN Estadísticas Matrícula por Departamentos) — `c_digo_deldepartamento` → **DIVIPOLA dept**.
- `enmx-7kvv` (MEN Matrícula Migrantes) — no DIVIPOLA codes. Only `codigo_pais_origen` (country).
- `v488-qa3u` (MEN Indicadores Educación Media) — `cod_dane_depto` → **DIVIPOLA dept**.
- `3y4s-dmxy` (MEN Indicadores Primera Infancia) — `cod_dane_depto`, `cod_dane_mun` → **DIVIPOLA**.

---

## 13. Final Revised Priority List

### Tier 1 — Core (immediate, with latent keys discovered)

| # | dataset_id | Name | Proven Join Keys | Why |
|---|---|---|---|---|
| 1 | **f789-7hwg** | SECOP I - Procesos de Compra Pública | nit_de_la_entidad, c_digo_de_la_entidad, identificacion_del_contratista, codigo_bpin, numero_de_proceso | THE missed dataset. 6.4M rows, 5 join keys at ~100%. |
| 2 | **wi7w-2nvm** | SECOP II - Ofertas Por Proceso | nit_entidad_compradora, nit_del_proveedor, id_del_proceso_de_compra, c_digo_entidad, c_digo_proveedor | 5 latent join keys. Bid-rigging detection. |
| 3 | **mfmm-jqmq** | SECOP II - Ejecución Contratos | identificadorcontrato | Already in registry but mis-parsed. |
| 4 | **ceth-n4bn** | Grupos de Proveedores - SECOP II | nit_grupo, nit_participante | Consortium/collusion mapping. |
| 5 | **hgi6-6wh3** | Proponentes por Proceso SECOP II | nit_entidad, codigo_entidad, nit_proveedor | Bidder networks. |
| 6 | **tauh-5jvn** | SECOP I - Proponentes | numero_contrato, id_proceso | SECOP I bidder data. |
| 7 | **3hdv-smhz** | TVEC - Compras por item | nit_entidad, nit_proveedor | Item-level gov procurement. |
| 8 | **rgxm-mmea** | TVEC Consolidado | nit_entidad, id_entidad, nit_proveedor | Aggregated e-store purchases. |
| 9 | **4n4q-k399** | Multas y Sanciones SECOP I | nit_entidad, documento_contratista | SECOP I sanctions. |
| 10 | **h7zv-k39x** | Universo de entidades | dm_institucion_cod_institucion, ccb_nit_inst, idmunicipio, iddepartamento | Master entity directory. |

### Tier 2 — High Value (newly discovered or promoted)

| # | dataset_id | Name | Latent Join Keys | Why |
|---|---|---|---|---|
| 11 | **5phs-yqfw** | Información de Gastos PGN | codigoentidad → entity code | Budget execution — direct corruption signal. |
| 12 | **br9a-gygu** | Ejecución Financiera de Regalías | codigobpin, codigodaneentidad → BPIN, DIVIPOLA | Royalties execution — high-risk fund flow. |
| 13 | **cf9k-55fw** | DNP-proyectos_datos_basicos | bpin, codigoentidadresponsable | Project master data. |
| 14 | **4ex9-j3n8** | SECOP II - Contacto Entidades y Proveedores | codigo_entidad, nit_entidad | Entity/supplier directory. |
| 15 | **h2yr-zfb2** | Subsidios De Vivienda Asignados | c_digo_divipola_departamento, c_digo_divipola_municipio | Housing subsidies by DIVIPOLA. |
| 16 | **s87b-tjcc** | Estadísticas Solicitudes Restitución | codigodanemunicipiopredio → DIVIPOLA | Land restitution by municipality. |
| 17 | **acs4-3wgp** | Detección Cultivos de Coca | coddepto, codmpio → DIVIPOLA | Coca cultivation — territorial risk scoring. |
| 18 | **tnbg-fy2n** | Producción de Bienes Nacionales | productor_nit → NIT | Producer NIT links to industrial producers. |
| 19 | **sasi-u68b** | Entidades Acreditadas Oct 2023 | numero_nit → NIT | Accredited entities with representative cedula. |
| 20 | **4ex9-j3n8** | SECOP II - Contacto Entidades y Proveedores | codigo_entidad, nit_entidad | Already listed above. |
| 21 | **xyy7-rn7p** | Quejas consumidores financieros | codigo_entidad | Consumer complaints = irregularity signal. |

### Tier 3 — Contextual (DIVIPOLA linkage only, education/social data)

| # | dataset_id | Name | Latent Keys | Why |
|---|---|---|---|---|
| 22 | **upr9-nkiz** | MEN Programas Educación Superior | codigodepartinstitucion, codigomunicipioinstitucion | DIVIPOLA-linked education data. |
| 23 | **y9ga-zwzy** | MEN Estadísticas Matrícula Municipios | c_digo_deldepartamento, c_digo_delmunicipio | DIVIPOLA-linked enrollment. |
| 24 | **nudc-7mev** | MEN Estadísticas Educación Municipio | c_digo_municipio, c_digo_departamento | DIVIPOLA-linked education stats. |
| 25 | **3y4s-dmxy** | MEN Indicadores Primera Infancia | cod_dane_depto, cod_dane_mun | DIVIPOLA-linked child indicators. |
| 26 | **xm4c-2rks** | Solicitudes de conciliación | sic_codigo_dane → DIVIPOLA | Justice access by municipality. |
| 27 | **5n52-6rih** | Población Afiliada | id_ccf → entity ID (CCF) | CCF coverage reference. |
| 28 | **cm2t-qreq** | Encuesta seguridad vial | ik_divipola → DIVIPOLA | Survey data (very wide). |

### Tier 4 — No Join Keys (skip for now)

The remaining 60+ datasets from the probe have **no entity code, NIT, DIVIPOLA, BPIN, contract, or process ID columns**. They use only descriptive text names for departments/municipalities/entities, or are aggregate statistics with no entity-level linkage. Examples:

- All "Entidades Acreditadas" duplicates (10+ copies with NIT but limited value beyond `cmgp-8z8t`/`sasi-u68b`)
- Justice text-only datasets (Arbitraje, Insolvencia, Demobilization, etc.)
- Archive inventories (SUIN-Juriscol, FOGACOOP, MinJusticia docs)
- Aggregate statistics (UGPP evasion rate, Cannabis licenses, USPEC food)
- Descriptive directories without codes (Consultorios Jurídicos, Casas de Justicia, Centros de Conciliación)

**These should be skipped unless entity name fuzzy-matching is added to the pipeline.**

---

## 14. Final Action Items (Consolidated)

1. **Fix triage normalization** to recognize these patterns:
   - `nit_de_la_entidad` → entity NIT
   - `c_digo_de_la_entidad` → entity code
   - `identificacion_del_contratista` → supplier NIT
   - `identificadorcontrato` → contract ID
   - `nit_entidad_compradora` → entity NIT
   - `nit_del_proveedor` → supplier NIT
   - `id_del_proceso_de_compra` → process ID
   - `c_digo_entidad` / `c_digo_proveedor` → entity/supplier code
   - `nit_grupo` / `nit_participante` → NIT
   - `codigoentidad` (camelCase, no underscore) → entity code
   - `codigobpin` → BPIN
   - `codigodaneentidad` → DIVIPOLA entity code
   - `codigoentidadresponsable` → entity code
   - `dm_institucion_cod_institucion` → entity code
   - `ccb_nit_inst` → NIT
   - `idmunicipio` / `iddepartamento` → DIVIPOLA
   - `sic_codigo_dane` → DIVIPOLA municipality code
   - `coddepto` / `codmpio` / `cod_dane_depto` / `cod_dane_mun` / `c_digo_divipola_*` → DIVIPOLA
   - `productor_nit` / `numero_nit` / `n_mero_nit` → NIT
   - `codigodanemunicipiopredio` → DIVIPOLA municipality code

2. **Re-probe row counts** for `9sue-ezhx`, `hgi6-6wh3`, `prdx-nxyp`, `wi7w-2nvm`.

3. **Wire already-in-registry fixes**: `wi7w-2nvm` (Ofertas) and `mfmm-jqmq` (Ejecución) with correct join key aliases.

4. **Consider demoting**: `wwhe-4sq8` (Ubicaciones Adicionales) and `dmgg-8hin` (Archivos Descarga) — both in registry but unused.

5. **Un-quarantine** `cwhv-7fnp` (Rubros Presupuestales) — has `id_contrato` at 5/5.

6. **Promote** `f789-7hwg` from `context_enrichment` to `ingest_priority`.

7. **Add normalization** for `mzgh-shtp` (DNP-ProyectosSGR) — `codigobpin` → BPIN.

8. **Promote** `5phs-yqfw`, `br9a-gygu`, `h7zv-k39x` from `no_join`/`large_no_join` to `ingest`.

9. **Add Tier 2 new discoveries**: `h2yr-zfb2` (Subsidios Vivienda), `s87b-tjcc` (Restitución), `acs4-3wgp` (Coca), `tnbg-fy2n` (Bienes Nacionales), `sasi-u68b` (Acreditadas).

10. **Add DIVIPOLA-linked Tier 3 datasets**: `upr9-nkiz`, `y9ga-zwzy`, `nudc-7mev`, `3y4s-dmxy`, `xm4c-2rks`.

---

## 15. Remaining Proven Join-Key Datasets (Review of 25 unreviewed)

These 25 datasets had join keys detected by the triage but were not individually reviewed until now.

### 15.1 Already reviewed in context (confirming)

| dataset_id | Name | Keys | Status |
|---|---|---|---|
| ps88-5e3v | SECOP I - Archivos Descarga | entity:`codigo_entidad` (5/5) | Already in registry (review_or_drop). 32M rows of doc metadata. Keep review_or_drop. |
| tauh-5jvn | SECOP I - Proponentes | contract+process | Already reviewed in §2.1. |
| cf9k-55fw | DNP-proyectos_datos_basicos | bpin | Already reviewed in §2.3. |
| xjxk-qhsc | Ejecución Presupuestal del PGN | entity:`codigo_entidad` | Already reviewed in §9.5. Summary-level PGN execution (554 rows). `ingest_if_useful`. |
| 4qkq-csdn | RUPS - Registro Único de Prestadores de Servicios Públicos | nit | Already reviewed. Service provider registry. **ingest_if_useful** — links NIT to public service providers. |
| 6cat-2gcs | 10.000 Empresas mas Grandes del País | nit | Already reviewed. Corporate financials by NIT. **ingest** — enriches entity financial profiles. |
| kg2d-yfyg | Listado de Entidades del Sector Solidario | nit | Already reviewed. Solidarity sector entities by NIT + codentidad. **ingest**. |
| s97v-q3tx | Superservicios - Inf. Comercial Usuarios NO Regulados | nit | Already reviewed. Utility billing data by NIT. **ingest_if_useful** — very granular (65K rows). Has `nit` + `car_t1556_codigodane`. |
| dd55-74ss | SUJETOS OBLIGADOS | nit | Probed now. AML/FT reporting entities registry. 161K rows. `nit` (entity NIT) + `razon_social` + `estado` + `departamento_judicial`. **ingest** — identifies entities obligated to report suspicious transactions. Direct corruption relevance. |
| r3d5-pipz | Balance de entidades publicas vigiladas por la SFC | entity:`codigo_entidad` | Probed now. SFC-supervised entity balance data. 886K rows. **ingest**. |
| tic6-rbue | Estados financieros entidades solidarias | entity:`codigo_entidad`, nit:`nit` | Already reviewed in §6.39. **ingest_if_useful** — row count timed out, re-probe needed. |
| mxk5-ce6w | CUIF por moneda Entidades vigiladas | entity:`codigo_entidad` | Probed now. 19.3M rows. Financial information by entity. **ingest** (lake-first). |
| pfdp-zks5 | NIIF - Estado de Situación Financiera | nit:`nit` | Probed now. 17.9M rows. Balance sheets by NIT. **ingest** (lake-first). |
| 6hqw-m3dm | NIIF - Carátula | nit:`nit` | Probed now. 8.7M rows. NIIF report headers. **ingest**. |
| prwj-nzxa | NIIF - Estado de Resultado Integral | nit:`nit` | Probed now. 7.3M rows. Income statements. **ingest** (lake-first). |
| ctcp-462n | NIIF - Estado de Flujo Efectivo | nit:`nit` | Probed now. 5.8M rows. Cash flows. **ingest** (lake-first). |
| y3gh-x5g7 | NIIF - Otro Resultado Integral | nit:`nit` | Probed now. 1.7M rows. Other comprehensive income. **ingest_if_useful**. |
| rvii-eis8 | Distribución de cartera por producto | entity:`codigo_entidad` | Probed now. 107K rows. Loan portfolio distribution per entity. Has detailed aging/mora columns. **ingest** — credit risk and overdue loan data per supervised entity = direct financial health indicator. |
| uawh-cjvi | Fondo de Pensiones Obligatorias y Cesantías | entity:`codigo_entidad` | Probed now. 94K rows. Pension fund data per entity. **ingest_if_useful**. |
| 7jfv-7spn | Montos y número de créditos aprobados o desembolsados | entity:`codigo_entidad` | Probed now. 50K rows. Credit data per entity. **ingest**. |

### 15.2 New or confirmed from this batch

| dataset_id | Name | Rows | Keys | Assessment |
|---|---|---:|---|---|
| **dd55-74ss** | SUJETOS OBLIGADOS | 161,771 | `nit` | **ingest**. AML/FT reporting entity registry. Identifies entities required to report suspicious transactions. |
| **thwd-ivmp** | Registro Nacional de Turismo - RNT | 686,967 | `nit`, DIVIPOLA:`cod_dpto`/`cod_mun` | **ingest_if_useful**. Tourism establishment registry by NIT + DIVIPOLA. Corruption risk: tourism concessions and licenses. |
| **ux6v-gpit** | Población Intramural INPEC por DIVIPOLA | 6,465 | DIVIPOLA:`codigo_municipio`/`codigo_departamento`, entity:`codigo_establecimiento` | **context_enrichment**. Prison population by facility + DIVIPOLA. Corruption relevance: overcrowding as governance failure indicator. |
| **xcga-ji4i** | Reporte movilización por municipio | 172,965 | DIVIPOLA:`cod_dpto`/`cod_mun` | **context_if_useful**. Financial transactions by municipality. Links to DIVIPOLA. |
| **24ny-2dhf** | Sectores Críticos por Exceso de Velocidad | 970 | DIVIPOLA:`divipola` | **skip**. Road safety only. |
| **rs3u-8r4q** | Sectores Críticos de Siniestralidad Vial | 316 | DIVIPOLA:`divipola` | **skip**. Road safety only. |
| **n5yy-8nav** | MEN Instituciones Educación Superior | 361 | `c_digo_instituci_n`, `n_mero_identificaci_n` (potential NIT), DIVIPOLA:`cod_departamento`/`cod_municipio` | **context_enrichment**. Higher education institutions directory. Has `n_mero_identificaci_n` which may be NIT. |
| **cfw5-qzt5** | MEN Establecimientos Educativos | 588,334 | DIVIPOLA:`cod_dane_departamento`/`cod_dane_municipio` | **context_if_useful**. School locations by DIVIPOLA. Very large. |
| **2v94-3ypi** | MEN Programas Educación Trabajo | 19,867 | DIVIPOLA:`cod_dpto`/`cod_mpio` | **context_if_useful**. Training programs. |
| **2c7k-9iru** | SENA Cupos Formación Profesional | 42,080 | DIVIPOLA:`codigo_departamento_curso`/`codigo_municipio_curso` | **context_if_useful**. SENA training slots. |
| **rvii-eis8** | Distribución cartera por producto | 107,721 | entity:`codigo_entidad` | **ingest**. Credit risk/overdue indicators per supervised entity. |

### 15.3 Confirmed Drop (education only, no corruption relevance)

| dataset_id | Name | Rows | Keys | Assessment |
|---|---|---:|---|---|
| 28vu-5tx7 | CERTIFICACIÓN Formación Profesional | 8,538 | entity? | Drop — SENA internal. |
| 5c2k-ahfc | MEN Número Bachilleres por ETC | 7,850 | DIVIPOLA | Drop — education stats only. |
| epkg-mphw | MEN Indicadores PAE | 121,379 | DIVIPOLA | Drop — school feeding stats. |
| j9sd-zau5 | MEN Sedes Educativas | 53,539 | DIVIPOLA | Drop — school locations. |
| 8cnh-7asj | Georeferenciación Centros SENA | 118 | DIVIPOLA | Drop — SENA campus geo. |
| ff7q-jcdw | Comercialización Materiales Metrología | 228 | ? | Drop — irrelevant. |
| upd7-iccu | Oferta Formativa Metrología | 1,186 | ? | Drop — irrelevant. |
| vv8g-8u9u | PROGRAMACIÓN ESPECÍFICA CURSOS SENA | 42,080 | DIVIPOLA | Drop — SENA enrollment. |
| rtxu-twjm | Distritos de Riego activos | 833 | ? | Drop — irrigation districts. |

---

## 16. FINAL Consolidated Priority (All 285+ datasets reviewed)

### Tier 1 — Core (13 datasets, immediate ingestion)

| # | ID | Name | Keys | Why |
|---|---|---|---|---|
| 1 | f789-7hwg | SECOP I - Procesos de Compra Pública | NIT, entity code, supplier ID, BPIN, process | THE missed dataset. 6.4M rows, 5 keys at 100%. |
| 2 | wi7w-2nvm | SECOP II - Ofertas Por Proceso | 5 latent keys | Already in registry but mis-parsed. |
| 3 | mfmm-jqmq | SECOP II - Ejecución Contratos | contract ID | Already in registry but mis-parsed. |
| 4 | ceth-n4bn | Grupos de Proveedores - SECOP II | NIT grupo+NIT participante | Consortium/collusion mapping. |
| 5 | hgi6-6wh3 | Proponentes por Proceso SECOP II | entity NIT+code, supplier NIT | Bidder networks. |
| 6 | tauh-5jvn | SECOP I - Proponentes | contract, process | SECOP I bidders. |
| 7 | 3hdv-smhz | TVEC - Compras por item | entity NIT, supplier NIT | Item-level procurement. |
| 8 | rgxm-mmea | TVEC Consolidado | entity NIT+code, supplier NIT | Aggregated e-store. |
| 9 | 4n4q-k399 | Multas y Sanciones SECOP I | entity NIT, supplier NIT | SECOP I sanctions. |
| 10 | h7zv-k39x | Universo de entidades | entity code, NIT, DIVIPOLA | Master entity directory. |
| 11 | 5phs-yqfw | Información de Gastos PGN | entity code | Budget execution — direct corruption signal. |
| 12 | br9a-gygu | Ejecución Financiera de Regalías | BPIN, DIVIPOLA entity | Royalties execution — high-risk fund flow. |
| 13 | dd55-74ss | SUJETOS OBLIGADOS | NIT | AML/FT reporting entity registry. |

### Tier 2 — High Value (21 datasets)

| # | ID | Name | Keys | Why |
|---|---|---|---|---|
| 14 | cf9k-55fw | DNP-proyectos_datos_basicos | BPIN, entity code | Project master data. |
| 15 | 4ex9-j3n8 | SECOP II - Contacto Entidades Proveedores | entity code, NIT | Entity/supplier directory. |
| 16 | e2u2-swiw | SECOP II - Modificaciones Procesos | entity NIT+code | Process modification audit. |
| 17 | b6m4-qgqv | SECOP II - PAA Encabezado | entity code | Annual procurement plans. |
| 18 | 36vw-pbq2 | SECOP I - Modificaciones Procesos | process ID | Process modification audit. |
| 19 | 9sue-ezhx | SECOPII - PAA Detalle | entity code, NIT | Procurement plan items. |
| 20 | prdx-nxyp | SECOP I - PAA Encabezado | DIVIPOLA, entity code+NIT | SECOP I PAA data. |
| 21 | xyy7-rn7p | Quejas consumidores financieros | entity code | Consumer complaints. |
| 22 | h2yr-zfb2 | Subsidios De Vivienda | DIVIPOLA dept+municipio | Housing subsidies. |
| 23 | s87b-tjcc | Restitución Municipios | DIVIPOLA municipality | Land restitution. |
| 24 | acs4-3wgp | Detección Cultivos de Coca | DIVIPOLA dept+municipio | Coca cultivation risk. |
| 25 | tnbg-fy2n | Producción Bienes Nacionales | NIT | Industrial producer registry. |
| 26 | thwd-ivmp | Registro Nacional de Turismo | NIT, DIVIPOLA | Tourism registry. |
| 27 | rvii-eis8 | Distribución cartera por producto | entity code | Credit risk per supervised entity. |
| 28 | 6cat-2gcs | 10.000 Empresas Grandes | NIT | Corporate financials. |
| 29 | kg2d-yfyg | Entidades Sector Solidario | NIT, entity code | Solidarity sector directory. |
| 30 | xjxk-qhsc | Ejecución Presupuestal PGN | entity code | PGN budget execution (summary). |
| 31 | r3d5-pipz | Balance entidades SFC | entity code | SFC-supervised entity balances. |
| 32 | 7jfv-7spn | Montos créditos aprobados | entity code | Credit data per entity. |
| 33 | mxk5-ce6w | CUIF Entidades vigiladas | entity code | Financial info by entity (19M rows). |
| 34 | pfdp-zks5 | NIIF - Situación Financiera | NIT | Balance sheets (18M rows). |

### Tier 3 — Contextual (18 datasets, DIVIPOLA/enrichment)

DIVIPOLA-linked education/social data, financial enrichment, and entity references.

### Tier 4 — Skip (confirmed no corruption relevance)

~60 datasets: education-only (SENA/MEN), road safety, weather, internal archives, SNA internal ops, cannabis aggregates, etc. No entity/NIT/BPIN/DIVIPOLA linkage or no corruption detection value.
