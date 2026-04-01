import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router";

import {
  ApiError,
  getSignal,
  listSignals,
  type SignalDefinition,
  type SignalDetailResponse,
  type SignalListItem,
} from "@/api/client";

import styles from "./Signals.module.css";

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

export function Signals() {
  const navigate = useNavigate();
  const { signalId } = useParams<{ signalId: string }>();

  const [signals, setSignals] = useState<SignalListItem[]>([]);
  const [registryMeta, setRegistryMeta] = useState<{ lastRunId?: string | null; lastRefreshedAt?: string | null }>({});
  const [detail, setDetail] = useState<SignalDetailResponse | null>(null);
  const [loadingList, setLoadingList] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoadingList(true);
    setError(null);
    listSignals()
      .then((response) => {
        if (cancelled) return;
        setSignals(response.signals);
        setRegistryMeta({
          lastRunId: response.last_run_id,
          lastRefreshedAt: response.last_refreshed_at,
        });
        const firstSignal = response.signals[0];
        if (!signalId && firstSignal) {
          navigate(`/app/signals/${firstSignal.id}`, { replace: true });
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "No fue posible cargar el registro de señales.");
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingList(false);
      });
    return () => {
      cancelled = true;
    };
  }, [navigate, signalId]);

  useEffect(() => {
    let cancelled = false;
    if (!signalId) {
      setDetail(null);
      return () => {
        cancelled = true;
      };
    }

    setLoadingDetail(true);
    getSignal(signalId)
      .then((response) => {
        if (!cancelled) {
          setDetail(response);
        }
      })
      .catch((loadError) => {
        if (cancelled) return;
        if (loadError instanceof ApiError && loadError.status === 404) {
          setDetail(null);
          setError("No encontramos esa señal.");
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "No fue posible cargar la señal.");
      })
      .finally(() => {
        if (!cancelled) setLoadingDetail(false);
      });

    return () => {
      cancelled = true;
    };
  }, [signalId]);

  const filteredSignals = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return signals;
    return signals.filter((signal) => {
      const haystack = [
        signal.title,
        signal.id,
        signal.category,
        signal.description,
      ].join(" ").toLowerCase();
      return haystack.includes(normalized);
    });
  }, [query, signals]);

  return (
    <div className={styles.page}>
      <aside className={styles.rail}>
        <div className={styles.railHeader}>
          <p className={styles.kicker}>Signal Registry</p>
          <h1 className={styles.title}>Señales publicables</h1>
          <p className={styles.subtitle}>
            Registro versionado de señales que el sistema puede materializar con evidencia pública y reglas explícitas.
          </p>
          <p className={styles.subtitle}>
            Última corrida {registryMeta.lastRunId ?? "—"} · {formatDate(registryMeta.lastRefreshedAt)}
          </p>
        </div>

        <label className={styles.searchBox}>
          <span>Filtrar</span>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="categoría, señal o fuente"
          />
        </label>

        {loadingList ? (
          <p className={styles.stateText}>Cargando registro…</p>
        ) : (
          <div className={styles.signalList}>
            {filteredSignals.map((signal) => (
              <button
                key={signal.id}
                type="button"
                onClick={() => navigate(`/app/signals/${signal.id}`)}
                className={`${styles.signalIndexCard} ${signalId === signal.id ? styles.signalIndexCardActive : ""}`}
              >
                <div className={styles.signalIndexTop}>
                  <span className={`${styles.severityPill} ${severityClass(signal.severity)}`}>{signal.severity}</span>
                  <span className={styles.hitCount}>{signal.hit_count}</span>
                </div>
                <strong>{signal.title}</strong>
                <span className={styles.signalIndexMeta}>{signal.category}</span>
                <small>{signal.scope_type}</small>
                <small>{signal.public_safe ? "Pública" : "Revisión"}</small>
              </button>
            ))}
            {!filteredSignals.length ? <p className={styles.stateText}>No hay señales que coincidan con ese filtro.</p> : null}
          </div>
        )}
      </aside>

      <section className={styles.sheet}>
        {error ? <p className={styles.error}>{error}</p> : null}
        {loadingDetail ? (
          <div className={styles.stateBlock}>Cargando ficha metodológica…</div>
        ) : detail ? (
          <>
            <header className={styles.sheetHeader}>
              <div>
                <p className={styles.sheetEyebrow}>{detail.definition.id}</p>
                <h2 className={styles.sheetTitle}>{detail.definition.title}</h2>
              </div>
              <div className={styles.sheetMeta}>
                <span className={`${styles.severityPill} ${severityClass(detail.definition.severity)}`}>
                  {detail.definition.severity}
                </span>
                <span className={styles.scopePill}>{detail.definition.category}</span>
                <span className={styles.scopePill}>{detail.definition.scope_type}</span>
                <span className={styles.scopePill}>{detail.definition.public_safe ? "Public safe" : "Review only"}</span>
              </div>
            </header>

            <p className={styles.lead}>{detail.definition.description}</p>

            <div className={styles.methodGrid}>
              <article className={styles.methodBlock}>
                <span className={styles.blockLabel}>Requisitos de identidad</span>
                <div className={styles.chipRow}>
                  {detail.definition.requires_identity.map((item) => (
                    <span key={item} className={styles.methodChip}>{item}</span>
                  ))}
                </div>
              </article>
              <article className={styles.methodBlock}>
                <span className={styles.blockLabel}>Fuentes requeridas</span>
                <div className={styles.chipRow}>
                  {detail.definition.sources_required.map((item) => (
                    <span key={item} className={styles.methodChip}>{item}</span>
                  ))}
                </div>
              </article>
              <article className={styles.methodBlock}>
                <span className={styles.blockLabel}>Compilador</span>
                <div className={styles.chipRow}>
                  <span className={styles.methodChip}>{detail.definition.runner?.kind ?? "pattern"}</span>
                  <span className={styles.methodChip}>{detail.definition.runner?.ref ?? detail.definition.pattern_id ?? "—"}</span>
                </div>
              </article>
              <article className={styles.methodBlock}>
                <span className={styles.blockLabel}>Deduplicación</span>
                <div className={styles.chipRow}>
                  {detail.definition.dedup_fields.map((item) => (
                    <span key={item} className={styles.methodChip}>{item}</span>
                  ))}
                </div>
              </article>
            </div>

            <section className={styles.samples}>
              <div className={styles.sectionHeader}>
                <div>
                  <span className={styles.blockLabel}>Muestras materializadas</span>
                  <h3 className={styles.sectionTitle}>Ejemplos de hits persistidos</h3>
                </div>
                <span className={styles.sectionNote}>{detail.sample_hits.length} muestras</span>
              </div>

              <div className={styles.sampleList}>
                {detail.sample_hits.map((hit) => (
                  <article key={hit.hit_id} className={styles.sampleCard}>
                    <div className={styles.sampleHeader}>
                      <div>
                        <strong className={styles.sampleTitle}>{hit.entity_key}</strong>
                        <p className={styles.sampleSummary}>{hit.description}</p>
                      </div>
                      <div className={styles.sampleMetrics}>
                        <span>{hit.evidence_count} soportes</span>
                        <strong>{Math.round(hit.score * 10) / 10}</strong>
                      </div>
                    </div>
                    <div className={styles.sampleMetaRow}>
                      <span>ID {Math.round(hit.identity_confidence * 100)}%</span>
                      <span>{hit.identity_match_type ?? hit.identity_quality ?? "identity:unknown"}</span>
                      {hit.scope_key ? <span>scope {hit.scope_key}</span> : null}
                      <span>{hit.scope_type}</span>
                      {hit.run_id ? <span>run {hit.run_id}</span> : null}
                      <span>seen {formatDate(hit.last_seen_at ?? hit.created_at)}</span>
                      <Link to={`/app/analysis/${hit.entity_id}`} className={styles.entityLink}>
                        abrir entidad
                      </Link>
                    </div>
                    {hit.evidence_items.length ? (
                      <ul className={styles.evidenceList}>
                        {hit.evidence_items.slice(0, 3).map((item) => (
                          <li key={item.item_id} className={styles.evidenceRow}>
                            {item.url ? (
                              <a href={item.url} target="_blank" rel="noreferrer" className={styles.evidenceLink}>
                                {item.label ?? item.url}
                              </a>
                            ) : (
                              <span className={styles.evidenceLink}>{item.label ?? item.record_id ?? item.item_id}</span>
                            )}
                            <small>{item.source_id ?? "evidencia"}</small>
                            <small>{item.item_type}</small>
                            {item.node_ref ? <small>{item.node_ref}</small> : null}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                  </article>
                ))}
                {!detail.sample_hits.length ? <p className={styles.stateText}>Todavía no hay muestras persistidas para esta señal.</p> : null}
              </div>
            </section>
          </>
        ) : (
          <div className={styles.stateBlock}>Seleccione una señal para ver su ficha metodológica.</div>
        )}
      </section>
    </div>
  );
}
