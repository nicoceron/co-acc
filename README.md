# co/acc: Colombia Open Graph Infrastructure

**An open-source graph infrastructure project that consolidates fragmented Colombian public datasets into a single queryable Neo4j graph, with the goal of making public information easier to access and investigate.**

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](LICENSE)
[![Data Sources: 30+](https://img.shields.io/badge/Data%20Sources-30+-green.svg)](docs/data-sources.md)

---

<img width="2610" height="1962" alt="Screenshot 2026-04-30 at 1 56 56 PM" src="https://github.com/user-attachments/assets/7137ee4f-98c5-4298-b55c-b54c7fedd379" />

## 🚀 Overview

`co/acc` (Colombia Acceleration) is a specialized graph-based platform re-engineered for the Colombian public data ecosystem. It transforms millions of disconnected records from official government portals ([datos.gov.co](https://www.datos.gov.co/)) into a unified "Knowledge Graph" where companies, people, contracts, and political office holders are linked by their official identifiers (NIT/Cédula).

### Why this matters
Colombian public data is transparent but **fragmented**. A contractor might appear in SECOP II, their campaign donations in Cuentas Claras, and their public office history in SIGEP. `co/acc` bridges these silos, allowing investigators to find "hidden" patterns like:
- **Donor-Official-Vendor Loops:** People who donate to a campaign, hold office, and also win contracts.
- **Same-ID Incompatibilities:** Public servants whose private companies are winning contracts with their own agencies.
- **Sanctioned Continuity:** Companies that continue winning bids despite active fiscal or disciplinary sanctions.

---

## 🛠 Key Features

- **30+ Curated Pipelines:** Automated ETL for SECOP (I, II, Integrated), SIGEP, SGR (Royalties), PACO, and more.
- **Unified Graph Schema:** A battle-tested Neo4j model that handles millions of nodes and relationships.
- **Interactive Explorer:** A React-based frontend to visualize entities and navigate their connections.
- **Detection Engine:** Pre-configured Cypher queries to surface 10+ types of risk-style signals.
- **High Performance:** Designed to scale to 10M+ nodes on consumer-grade hardware.

---

## 📊 The Data Universe

We currently track and integrate **30 datasets**, including:

| Category | Key Sources |
| :--- | :--- |
| **Contracts** | SECOP I & II, Integrated Awards, Offers, Invoices, Additions |
| **Public Sector** | SIGEP (Public Servants), Sensitive Positions, Asset Disclosures |
| **Politics** | Cuentas Claras (Campaign Finance), Conflict of Interest Disclosures |
| **Risk/Sanctions** | PACO (Red Flags), SECOP Sanctions, Fiscal/Disciplinary Feeds |
| **Budget** | SGR (Royalties), PGN (National Budget Commitments) |
| **Corporate** | RUES (Chambers of Commerce), Supersociedades (Top 1000 Companies) |

---

## ⚡ Quick Start

### Prerequisites
- Docker & Docker Compose
- At least 8GB RAM recommended

### Setup
```bash
# 1. Clone and enter
git clone https://github.com/World-Open-Graph/co-acc.git
cd co-acc

# 2. Configure environment
cp .env.example .env

# 3. Launch the stack (Neo4j, API, Frontend)
docker compose up -d --build

# 4. Seed with demo data
bash infra/scripts/seed-dev.sh
```

### Access Points
- **Frontend Explorer:** `http://localhost:3100`
- **API Documentation:** `http://localhost:8000/docs`
- **Neo4j Browser:** `http://localhost:7474` (User: `neo4j`, Pass: your .env value)

---

## 🔍 Investigation Workflow

1. **Download:** `make download-secop-integrado` (or use `etl/scripts/` directly).
2. **Ingest:** `make etl-all` to run the full pipeline and link the data into Neo4j.
3. **Query:** Use the "Watchlist" feature in the UI or run custom Cypher queries in Neo4j.

---

## Legal & Ethics

This project is governed by strict ethical and legal standards for public data processing:

- **[ETHICS.md](ETHICS.md):** Ethical guidelines for data-driven investigations.
- **[LGPD.md](LGPD.md):** Compliance with data protection principles (Ley 1581 / LGPD).
- **[PRIVACY.md](PRIVACY.md):** Public-surface privacy rules and redaction policies.
- **[SECURITY.md](SECURITY.md):** Security policy and vulnerability reporting.
- **[ABUSE_RESPONSE.md](ABUSE_RESPONSE.md):** Procedures for reporting and responding to data abuse.
- **[TERMS.md](TERMS.md):** Terms of use for the open-graph infrastructure.
- **[DISCLAIMER.md](DISCLAIMER.md):** Legal disclaimers regarding official data sources.

---

## 📜 License

Distributed under the **AGPL-3.0 License**. See `LICENSE` for more information.

---

*Part of the World Open Graph initiative.*
