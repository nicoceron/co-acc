import { ArrowLeft, ExternalLink, Gauge } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";

import {
  getCase,
  listCases,
  type CaseAnomalyScore,
  type CaseResponse,
  type CaseSummary,
} from "@/api/client";

import { DataStatus, EmptyState, Frame, Pill, Rule } from "../components/ui";
import { formatNumber } from "../lib/format";
import type { AtlasDataStatus } from "../lib/useAtlasData";

const FEATURE_LABELS: Record<string, string> = {
  log_value: "valor contractual inusual",
  log_value_z_buyer: "valor alejado de los contratos del comprador",
  log_value_z_modality: "valor alejado de la modalidad de contratación",
  buyer_supplier_concentration: "concentración comprador-proveedor",
  n_prior_contracts_12mo_buyer: "volumen reciente de contratos del comprador",
  n_prior_contracts_12mo_supplier: "historial reciente del proveedor",
  share_of_buyer_total_value_12mo: "participación en el gasto reciente del comprador",
  timing_anomaly_score: "fecha de firma atípica",
  modality_value_mismatch: "modalidad y valor poco habituales",
  single_bidder: "único oferente reportado",
};

function featureLabel(feature: string): string {
  return FEATURE_LABELS[feature] ?? feature.replaceAll("_", " ");
}

function formatCop(value?: number | null): string {
  if (value == null) return "—";
  return new Intl.NumberFormat("es-CO", {
    currency: "COP",
    maximumFractionDigits: 0,
    style: "currency",
  }).format(value);
}

function scoreConfidence(value: string): string {
  const labels: Record<string, string> = {
    high: "alta",
    low: "baja",
    standard: "estándar",
    unknown: "no disponible",
  };
  return labels[value] ?? value;
}

function ContractFacts({ score }: { score: CaseAnomalyScore }) {
  return (
    <div className="co-spec-list">
      <span><strong>contrato</strong>{score.contract_reference || score.contract_id}</span>
      <span><strong>proveedor</strong>{score.supplier_name || "no disponible"}</span>
      <span><strong>NIT proveedor</strong>{score.supplier_document_id || "no disponible"}</span>
      <span><strong>comprador</strong>{score.buyer_name || "no disponible"}</span>
      <span><strong>valor</strong>{formatCop(score.contract_value)}</span>
      <span><strong>firma</strong>{score.signing_date?.slice(0, 10) || "no disponible"}</span>
    </div>
  );
}

export function PrioritizedContractsPage() {
  const [cases, setCases] = useState<CaseSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState<AtlasDataStatus>("loading");

  useEffect(() => {
    let active = true;
    listCases(1, 20)
      .then((response) => {
        if (!active) return;
        setCases(response.cases.filter((item) => item.anomaly_score));
        setTotal(response.total);
        setStatus("live");
      })
      .catch(() => {
        if (!active) return;
        setCases([]);
        setStatus("unavailable");
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <main className="co-container co-signals">
      <header className="co-workspace-head">
        <div>
          <Rule accent>IA mínima · Isolation Forest</Rule>
          <h1>Contratos priorizados</h1>
          <p>
            Ranking no supervisado de actividad contractual inusual. El puntaje no mide
            corrupción, culpabilidad ni ilegalidad.
          </p>
        </div>
        <DataStatus status={status} />
      </header>

      <div className="co-table-wrap">
        <table className="co-table">
          <thead>
            <tr>
              <th>contrato</th>
              <th>proveedor / comprador</th>
              <th>valor / firma</th>
              <th>anomalía</th>
              <th>tres desviaciones principales</th>
              <th>fuente</th>
            </tr>
          </thead>
          <tbody>
            {cases.map((item) => {
              const score = item.anomaly_score;
              if (!score) return null;
              return (
                <tr key={item.id}>
                  <td>
                    <Link className="co-mono" to={`/priorizados/${item.id}`}>
                      {score.contract_reference || score.contract_id}
                    </Link>
                    <span className="co-row-sub">run {score.score_run_id}</span>
                  </td>
                  <td>
                    {score.supplier_name || "Proveedor no disponible"}
                    <span className="co-row-sub">{score.buyer_name || "Comprador no disponible"}</span>
                  </td>
                  <td>
                    {formatCop(score.contract_value)}
                    <span className="co-row-sub">{score.signing_date?.slice(0, 10) || "fecha no disponible"}</span>
                  </td>
                  <td className="co-num">
                    {score.score.toFixed(3)}
                    <span className="co-row-sub">confianza {scoreConfidence(score.score_confidence)}</span>
                  </td>
                  <td>{score.top_features.slice(0, 3).map(featureLabel).join(" · ")}</td>
                  <td>
                    {score.process_url ? (
                      <a href={score.process_url} target="_blank" rel="noreferrer">
                        SECOP <ExternalLink size={13} />
                      </a>
                    ) : "sin URL directa"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {status === "loading" ? (
          <EmptyState title="Cargando ranking" body="Leyendo el último scoring materializado." />
        ) : !cases.length ? (
          <EmptyState
            title={status === "unavailable" ? "Ranking no disponible" : "Sin contratos priorizados"}
            body="La aplicación no sustituye esta lista con datos de demostración."
          />
        ) : null}
      </div>

      <div className="co-inline-metrics">
        <span><Gauge size={14} /> {cases.length} mostrados</span>
        <span>{formatNumber(total)} contratos evaluados</span>
        <span>puntaje normalizado 0–1</span>
      </div>
    </main>
  );
}

export function PrioritizedContractDetailPage() {
  const { caseId } = useParams();
  const [detail, setDetail] = useState<CaseResponse | null>(null);
  const [status, setStatus] = useState<AtlasDataStatus>("loading");

  useEffect(() => {
    let active = true;
    if (!caseId) {
      setStatus("unavailable");
      return () => {
        active = false;
      };
    }
    getCase(caseId)
      .then((response) => {
        if (!active) return;
        setDetail(response);
        setStatus("live");
      })
      .catch(() => {
        if (!active) return;
        setDetail(null);
        setStatus("unavailable");
      });
    return () => {
      active = false;
    };
  }, [caseId]);

  const evidence = useMemo(
    () => detail?.evidence_bundles.flatMap((bundle) => bundle.evidence_items) ?? [],
    [detail],
  );
  const score = detail?.anomaly_score;

  if (!detail || !score) {
    return (
      <main className="co-container co-signal-detail">
        <Link className="co-button co-button--ghost" to="/priorizados">
          <ArrowLeft size={16} /> Contratos priorizados
        </Link>
        <EmptyState
          title={status === "loading" ? "Cargando contrato" : "Contrato no disponible"}
          body="No existe un resultado materializado para este identificador."
        />
      </main>
    );
  }

  return (
    <main className="co-container co-signal-detail">
      <Link className="co-button co-button--ghost" to="/priorizados">
        <ArrowLeft size={16} /> Contratos priorizados
      </Link>
      <header className="co-page-head">
        <Rule accent>{score.contract_id} · {score.score_run_id}</Rule>
        <h1>{detail.title}</h1>
        <p>Priorización estadística para revisión documental; no es una conclusión legal.</p>
        <div className="co-action-row">
          <Pill tone="accent">anomalía {score.score.toFixed(3)}</Pill>
          <Pill>confianza del score {scoreConfidence(score.score_confidence)}</Pill>
          <DataStatus status={status} />
        </div>
      </header>

      <div className="co-signal-detail__grid">
        <Frame coord="CONTRATO · A-01"><ContractFacts score={score} /></Frame>
        <Frame coord="MODELO · A-02">
          <div className="co-spec-list">
            <span><strong>modelo</strong>{score.model_run_id || "no disponible"}</span>
            <span><strong>features</strong>{score.feature_run_id || "no disponible"}</span>
            <span><strong>scoring</strong>{score.scored_at || "no disponible"}</span>
            <span><strong>fuente</strong>{score.source_id || "SECOP II"}</span>
          </div>
        </Frame>
      </div>

      <Frame coord="DESVIACIONES · A-03">
        <h2>Tres desviaciones principales</h2>
        <ol>
          {score.top_features.slice(0, 3).map((feature) => (
            <li key={feature}>{featureLabel(feature)}</li>
          ))}
        </ol>
      </Frame>

      <Frame coord="EVIDENCIA · A-04">
        <div className="co-frame-title">
          <div>
            <h2>Registro oficial</h2>
            <span>{evidence.length} referencias conservadas</span>
          </div>
        </div>
        <div className="co-evidence-list">
          {evidence.map((item) => (
            <article key={item.item_id}>
              <ExternalLink size={17} />
              <div>
                {item.url ? (
                  <a href={item.url} target="_blank" rel="noreferrer">
                    {item.label || item.record_id || "Abrir registro SECOP"}
                  </a>
                ) : <strong>{item.label || item.record_id || "Referencia sin URL directa"}</strong>}
                <span>{item.source_id || "fuente no atribuida"} · {item.observed_at || "fecha no disponible"}</span>
                <span>{item.row_selector || item.node_ref || item.file_selector || "selector no disponible"}</span>
              </div>
            </article>
          ))}
        </div>
      </Frame>
    </main>
  );
}
