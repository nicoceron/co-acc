import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..", "..");
const contractPath = resolve(root, "docs", "contracts", "api.openapi.yaml");
const clientPath = resolve(root, "frontend", "src", "api", "client.ts");

const contract = readFileSync(contractPath, "utf8");
const client = readFileSync(clientPath, "utf8");

const requiredContractFragments = [
  "openapi: 3.1.0",
  "/api/v1/cases/",
  "/api/v1/cases/{case_id}",
  "/api/v1/agent/query",
  "AgentQueryRequest:",
  "AgentQueryResponse:",
  "AgentCitation:",
  "AgentSubgraph:",
];

const requiredClientFragments = [
  "export interface AgentQueryRequest",
  "export interface AgentQueryResponse",
  "export function queryAgent",
  '"/api/v1/agent/query"',
];

const missing = [
  ...requiredContractFragments
    .filter((fragment) => !contract.includes(fragment))
    .map((fragment) => `${contractPath}: ${fragment}`),
  ...requiredClientFragments
    .filter((fragment) => !client.includes(fragment))
    .map((fragment) => `${clientPath}: ${fragment}`),
];

if (missing.length > 0) {
  console.error("API contract check failed. Missing fragments:");
  for (const item of missing) {
    console.error(`- ${item}`);
  }
  process.exit(1);
}

console.log("API contract check passed.");
