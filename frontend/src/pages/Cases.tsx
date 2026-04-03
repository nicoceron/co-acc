import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";

import {
  ApiError,
  getCase,
  listCases,
  refreshCase,
  type CaseResponse,
  type CaseSummary,
  type SignalDefinition,
} from "@/api/client";

import styles from "./Cases.module.css";

function statusLabel(status: string): string {
  if (status === "published") return "Publicado";
  if (status === "reviewed") return "Revisado";
  if (status === "triaged") return "Triaged";
  return "Nuevo";
}

function severityClass(severity: SignalDefinition["severity"]): string {
  if (severity === "critical") return styles.severityCritical ?? "";
  if (severity === "high") return styles.severityHigh ?? "";
  if (severity === "medium") return styles.severityMedium ?? "";
  return styles.severityLow ?? "";
}

function formatDate(value?: string | null): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

export function Cases() {
  const navigate = useNavigate();
  const { caseId } = useParams<{ caseId: string }>();

  const [cases, setCases] = useState<CaseSummary[]>([]);
  const [caseDetail, setCaseDetail] = useState<CaseResponse | null>(null);
  const [loadingList, setLoadingList] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoadingList(true);
    listCases()
      .then((response) => {
        if (cancelled) return;
        setCases(response.cases);
        const firstCase = response.cases[0];
        if (!caseId && firstCase) {
          navigate(`/app/cases/${firstCase.id}`, { replace: true });
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "No fue posible cargar los casos.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingList(false);
      });
    return () => {
      cancelled = true;
    };
  }, [caseId, navigate]);

  useEffect(() => {
    let cancelled = false;
    if (!caseId) {
      setCaseDetail(null);
      return () => {
        cancelled = true;
      };
    }
    setLoadingDetail(true);
    setError(null);
    getCase(caseId)
      .then((response) => {
        if (!cancelled) setCaseDetail(response);
      })
      .catch((loadError) => {
        if (cancelled) return;
        if (loadError instanceof ApiError && loadError.status === 404) {
          setCaseDetail(null);
          setError("No encontramos ese caso.");
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "No fue posible cargar el caso.");
      })
      .finally(() => {
        if (!cancelled) setLoadingDetail(false);
      });
    return () => {
      cancelled = true;
    };
  }, [caseId]);

  const selectedSummary = useMemo(
    () => cases.find((item) => item.id === caseId) ?? null,
    [cases, caseId],
  );

  async function handleRefreshCase() {
    if (!caseId) return;
    setRefreshing(true);
    setError(null);
    try {
      const response = await refreshCase(caseId);
      setCaseDetail(response);
      setCases((current) =>
        current.map((item) => (
          item.id === response.id
            ? {
                ...item,
                signal_count: response.signal_count,
                public_signal_count: response.public_signal_count,
                last_refreshed_at: response.last_refreshed_at,
                last_run_id: response.last_run_id,
                stale: response.stale,
              }
            : item
        )),
      );
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : "No fue posible refrescar el caso.");
    } finally {
      setRefreshing(false);
    }
  }

  return (
    <div className={styles.page}>
      <section className={styles.board}>
        <header className={styles.boardHeader}>
          <p className={styles.kicker}>Case Board</p>
          <h1 className={styles.title}>Dossiers de revisión</h1>
          <p className={styles.subtitle}>
            Cada caso agrupa señales persistidas, eventos y soportes documentales para revisión editorial o analítica.
          </p>
        </header>

        {loadingList ? (
          <p className={styles.stateText}>Cargando casos…</p>
        ) : (
          <div className={styles.caseList}>
            {cases.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() => navigate(`/app/cases/${item.id}`)}
                className={`${styles.caseCard} ${caseId === item.id ? styles.caseCardActive : ""}`}
              >
                <div className={styles.caseCardTop}>
                  <span className={styles.statusPill}>{statusLabel(item.status)}</span>
                  <span className={styles.countPill}>{item.signal_count}</span>
                </div>
                <strong>{item.title}</strong>
                <p>{item.description || "Sin narrativa todavía."}</p>
                <div className={styles.caseCardMeta}>
                  <span>{item.entity_ids.length} entidades</span>
                  <span>{item.public_signal_count} públicas</span>
                  <span>{item.stale ? "stale" : "fresh"}</span>
                  <span>{formatDate(item.last_refreshed_at)}</span>
                </div>
              </button>
            ))}
            {!cases.length ? <p className={styles.stateText}>Aún no hay casos registrados.</p> : null}
          </div>
        )}
      </section>

      <section className={styles.detail}>
        {error ? <p className={styles.error}>{error}</p> : null}
        {loadingDetail ? (
          <div className={styles.emptyState}>Cargando dossier…</div>
        ) : caseDetail ? (
          <>
            <header className={styles.detailHeader}>
              <div>
                <p className={styles.detailEyebrow}>Case {caseDetail.id}</p>
                <h2 className={styles.detailTitle}>{caseDetail.title}</h2>
                <p className={styles.detailLead}>{caseDetail.description || "Sin descripción curada todavía."}</p>
                <div className={styles.compilerMeta}>
                  <span>{caseDetail.stale ? "stale" : "fresh"}</span>
                  <span>run {caseDetail.last_run_id ?? "—"}</span>
                  <span>{formatDate(caseDetail.last_refreshed_at)}</span>
                </div>
              </div>
              <div className={styles.detailStats}>
                <div>
                  <span>señales</span>
                  <strong>{caseDetail.signal_count}</strong>
                </div>
                <div>
                  <span>públicas</span>
                  <strong>{caseDetail.public_signal_count}</strong>
                </div>
                <button
                  type="button"
                  onClick={() => void handleRefreshCase()}
                  className={styles.refreshButton}
                  disabled={refreshing}
                >
                  {refreshing ? "refrescando…" : "refrescar dossier"}
                </button>
              </div>
            </header>

            <div className={styles.linkRow}>
              <Link to={`/app/investigations/${caseDetail.id}`} className={styles.workspaceLink}>
                abrir workspace
              </Link>
              {selectedSummary?.entity_ids?.map((entityId) => (
                <Link key={entityId} to={`/app/analysis/${entityId}`} className={styles.workspaceLink}>
                  {entityId}
                </Link>
              ))}
            </div>

            <div className={styles.columns}>
              <section className={styles.column}>
                <div className={styles.sectionHeader}>
                  <span className={styles.sectionKicker}>Signals</span>
                  <h3>Señales activas</h3>
                </div>
                <div className={styles.signalList}>
                  {caseDetail.signals.map((signal) => (
                    <article key={signal.hit_id} className={styles.signalCard}>
                      <div className={styles.signalTop}>
                        <span className={`${styles.severityPill} ${severityClass(signal.severity)}`}>{signal.severity}</span>
                        <span className={styles.identityPill}>{Math.round(signal.identity_confidence * 100)}% ID</span>
                      </div>
                      <strong>{signal.title}</strong>
                      <p>{signal.description}</p>
                      <div className={styles.signalMeta}>
                        <span>{signal.entity_key}</span>
                        {signal.scope_key ? <span>{signal.scope_key}</span> : null}
                        <span>{signal.scope_type}</span>
                        <span>{signal.identity_match_type ?? signal.identity_quality ?? "identity:unknown"}</span>
                        {signal.run_id ? <span>{signal.run_id}</span> : null}
                        <span>{signal.evidence_count} soportes</span>
                      </div>
                    </article>
                  ))}
                  {!caseDetail.signals.length ? <p className={styles.stateText}>Este caso todavía no tiene señales materializadas.</p> : null}
                </div>
              </section>

              <section className={styles.column}>
                <div className={styles.sectionHeader}>
                  <span className={styles.sectionKicker}>Timeline</span>
                  <h3>Eventos y soporte</h3>
                </div>
                <div className={styles.eventList}>
                  {caseDetail.events.map((event) => (
                    <div key={event.id} className={styles.eventRow}>
                      <span className={styles.eventLabel}>{event.label}</span>
                      <span className={styles.eventDate}>
                        {new Date(event.date).toLocaleString()}
                        {typeof event.bundle_document_count === "number" ? ` · ${event.bundle_document_count} docs` : ""}
                      </span>
                    </div>
                  ))}
                </div>
                <div className={styles.bundleList}>
                  {caseDetail.evidence_bundles.map((bundle) => (
                    <article key={bundle.bundle_id} className={styles.bundleCard}>
                      <strong>{bundle.headline}</strong>
                      <div className={styles.bundleMeta}>
                        {bundle.source_list.join(" · ") || "sin fuente"}
                      </div>
                      <ul className={styles.bundleItems}>
                        {bundle.evidence_items.slice(0, 3).map((item) => (
                          <li key={item.item_id} className={styles.bundleItem}>
                            {item.url ? (
                              <a href={item.url} target="_blank" rel="noreferrer" className={styles.bundleLink}>
                                {item.label ?? item.url}
                              </a>
                            ) : (
                              <span className={styles.bundleLink}>{item.label ?? item.record_id ?? item.item_id}</span>
                            )}
                            <small className={styles.bundleMeta}>
                              {[item.source_id, item.item_type, item.identity_match_type, item.node_ref]
                                .filter(Boolean)
                                .join(" · ") || "sin metadatos"}
                            </small>
                          </li>
                        ))}
                      </ul>
                    </article>
                  ))}
                </div>
              </section>
            </div>
          </>
        ) : (
          <div className={styles.emptyState}>Seleccione un caso para ver el dossier materializado.</div>
        )}
      </section>
    </div>
  );
}
