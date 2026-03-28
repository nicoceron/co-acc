# Colombia Corruption Practices Research

Date: 2026-03-19

Scope: external web research on recurring corruption practices in Colombia, using official institutions and high-credibility civil-society sources to identify graph patterns that are still missing or under-modeled in CO-ACC.

## Sources used

- Transparencia por Colombia, "Tercer Informe Elecciones y Contratos"  
  https://transparenciacolombia.org.co/tercer-informe-elecciones-y-contratos/
- Transparencia por Colombia, recommendations on public contracting reform and anti-`pliegos sastre` controls  
  https://transparenciacolombia.org.co/wp-content/uploads/2024/02/Recomendaciones-PL-095-23s.-Contratacion-Estatal.pdf
- MOE, warning on interadministrative agreements during election windows  
  https://moe.org.co/en/eliminar-restricciones-en-la-ley-de-garantias-inclina-la-cancha-hacia-el-clientelismo-moe/
- Transparencia por Colombia, corruption risks in the health sector reform discussion  
  https://transparenciacolombia.org.co/riesgos-de-corrupcion-en-la-reforma-a-la-salud/
- Función Pública, protocol for identifying corruption risks in public procedures and services  
  https://www.funcionpublica.gov.co/eva/es/protocolo_corrupcion
- Transparencia por Colombia, contracting in peace implementation and the budget-to-contract traceability gap  
  https://transparenciacolombia.org.co/segundo-informe-contratacion-publica-acuerdo-paz/

## Practice inventory

### 1. Campaign money followed by public contracts

This is not a niche edge case. Transparencia por Colombia explicitly studies the relationship between private campaign financing and later public contracting as a recurring corruption risk, especially where private interests may later influence public management.

Graph implication:
- donor -> candidate/elected official -> buyer -> contract -> supplier
- donor company -> supplier company overlap
- donor person -> officer/legal representative -> supplier overlap

Already covered:
- `donor_official_vendor_loop`
- `company_donor_vendor_overlap`

Still worth expanding:
- time-window scoring between donation date and award date
- campaign-finance concentration by buyer, territory, and elected coalition

### 2. Interadministrative agreements used as discretionary electoral channels

MOE warns that interadministrative agreements are risky in election periods because they can move public resources with more discretion and fewer controls, creating clientelist or proselitist risk.

Graph implication:
- entity A -> interadministrative agreement -> entity B -> downstream contract -> supplier
- election window -> agreement execution -> downstream procurement
- territory -> agreement influx -> contracting spike

Dataset implications:
- `s484-c9k3`
- `ityv-bxct`
- `jgra-rz2t`
- SECOP buyer/contract timelines

Status:
- not yet implemented in the public graph

### 3. Tailored specifications, low competition, and simulated competition

Transparencia's contracting recommendations continue to treat `pliegos sastre` and weakened competition controls as central corruption risks. This lines up with the procurement patterns already visible in SECOP.

Graph implication:
- repeated single-bidder awards
- same buyer + same supplier + same category + repeated low competition
- same cluster of losing bidders around the same winner

Already covered:
- `low_competition_bidding`
- buyer and territory concentration alerts

Still worth expanding:
- repeated losing-bidder network
- post-award redesign after low-competition processes

### 4. Suspensions, stalled execution, and post-award manipulation

Recurring suspensions are a concrete risk signal for stalled works and execution distortion, especially when they coexist with invoicing, additions, or later modifications. This is one of the clearest bridges between "elefantes blancos" narratives and machine-detectable contract behavior.

Graph implication:
- contract -> suspension events
- contract -> invoices during low progress
- contract -> additions/modifications after suspensions

Datasets:
- `u99c-7mfm`
- `ibyt-yi2f`
- `mfmm-jqmq`
- `cb9c-h8sn`
- `u8cx-r425`

Status:
- implemented on 2026-03-19 as `secop_contract_suspensions`
- new public pattern: `contract_suspension_stacking`

### 5. Budget-to-contract traceability gaps

Transparencia's peace-implementation contracting work highlights a repeated structural problem: public budgets are allocated, but full downstream contractual traceability is weak. That creates room for laundering, fragmentation, or opacity between budget appropriation and supplier benefit.

Graph implication:
- budget/program -> BPIN/project -> process -> contract -> supplier
- project resource origin -> downstream award concentration
- territorial funding spike -> contract spike

Partially covered:
- `funding_spike_then_awards`
- SGR and PTE layers

Still worth expanding:
- `d9na-abhe`
- `3xwx-53wt`
- executor mismatch and beneficiary-footprint plausibility on DNP datasets

### 6. Health-sector corruption: ghost beneficiaries, medicine delivery fraud, political capture

Transparencia's health-sector analysis points to irregular medication supply, fictitious beneficiaries, overpricing, and political interference in managerial appointments. This matters because health corruption is not only about procurement; it also involves beneficiary rolls, operational control, and political appointment chains.

Graph implication:
- sanctioned health provider + contracts
- health provider + public official overlap
- beneficiary footprint vs service-site footprint
- medicine or benefit flows without plausible territorial or service capacity

Already covered:
- `sanctioned_health_operator_overlap`

Still worth expanding:
- BDUA and health population plausibility
- territorial appointment/clientelism around health institutions

### 7. Routine procedures, permits, and licenses

Función Pública explicitly treats public procedures and services as a corruption-risk domain. This supports moving beyond procurement-only logic into permits, environmental licensing, mining formalization, and expediente handling.

Graph implication:
- applicant -> representative -> expediente -> authority -> permit decision
- same representative across multiple permits and contracts
- permit approval timing around projects, contracts, or political cycles

Datasets from the catalog memo:
- `bxkb-7j6i`
- `si2v-pbq5`
- `42ha-fhvj`
- `7amp-4swy`
- `xzu3-gnau`
- `f385-sqmw`
- `xjzv-xx6n`
- `74ct-m5y8`

Status:
- not yet implemented in the production graph
- high-value backlog, especially for mining and environmental favoritism

## Best net-new hidden patterns

These are the strongest additions not fully modeled before this pass:

1. `contract_suspension_stacking`
   - repeated suspensions on the same supplier's contract portfolio
   - especially strong when paired with invoices, additions, or low execution

2. interadministrative-election channel
   - agreement-driven resource movement near elections followed by downstream supplier awards

3. BPIN laundering bridge
   - project/BPIN identifiers connecting awards and contracts back to project budgets

4. permit favoritism / expediente capture
   - repeated permit approvals, formalization steps, or administrative files around the same people or companies

5. beneficiary-footprint mismatch
   - project or social-program beneficiary claims that do not line up with territorial or service capacity

## Implementation priority

Add now:
- `u99c-7mfm` suspensions, because it is high-signal, easy to join by contract ID, and directly improves stalled-work detection

Add next:
- `uymx-8p3j` plan de pagos
- `d9na-abhe` BPIN por proceso
- `s484-c9k3` and `ityv-bxct` interadministrative agreements

Keep as backlog until the graph can support them properly:
- permit and mining expediente families
- beneficiary-footprint plausibility layers
