import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router";

import { RefreshCcw } from "lucide-react";

import type { EntitySignalsResponse, SignalHit } from "@/api/client";

import styles from "./EntitySignalsView.module.css";

interface EntitySignalsViewProps {
  signals: EntitySignalsResponse | null;
  loading: boolean;
  error: string | null;
  onRefresh: () => void;
}

function severityClass(severity: SignalHit["severity"]): string {
  if (severity === "critical") return styles.severityCritical ?? "";
  if (severity === "high") return styles.severityHigh ?? "";
  if (severity === "medium") return styles.severityMedium ?? "";
  return styles.severityLow ?? "";
}

function severityRank(severity: SignalHit["severity"]): number {
  if (severity === "critical") return 4;
  if (severity === "high") return 3;
  if (severity === "medium") return 2;
  return 1;
}

function formatDate(value?: string | null): string {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

export function EntitySignalsView({
  signals,
  loading,
  error,
  onRefresh,
}: EntitySignalsViewProps) {
  const { t } = useTranslation();

  const stats = useMemo(() => {
    const items = signals?.signals ?? [];
    const publicSafe = items.filter((item) => item.public_safe).length;
    const reviewerOnly = items.filter((item) => item.reviewer_only).length;
    const evidenceItems = items.reduce((total, item) => total + item.evidence_count, 0);
    const uniqueSources = new Set(
      items.flatMap((item) => item.sources.map((source) => source.database)).filter(Boolean),
    );
    const topSeverity = items.reduce<SignalHit["severity"] | null>((current, item) => {
      if (!current) return item.severity;
      return severityRank(item.severity) > severityRank(current) ? item.severity : current;
    }, null);

    return {
      total: items.length,
      publicSafe,
      reviewerOnly,
      evidenceItems,
      sourceCount: uniqueSources.size,
      topSeverity,
    };
  }, [signals]);

  return (
    <section className={styles.wrap} aria-busy={loading}>
      <header className={styles.hero}>
        <div className={styles.heroCopy}>
          <p className={styles.kicker}>{t("analysis.signalsEyebrow")}</p>
          <h2 className={styles.title}>{t("analysis.signalsTitle")}</h2>
          <p className={styles.lead}>{t("analysis.signalsLead")}</p>
        </div>
        <button
          type="button"
          onClick={onRefresh}
          className={styles.refreshButton}
          disabled={loading}
        >
          <RefreshCcw size={16} />
          <span>{loading ? t("analysis.loadingSignals") : t("analysis.refreshSignals")}</span>
        </button>
      </header>

      <div className={styles.metricGrid}>
        <article className={styles.metric}>
          <span className={styles.metricLabel}>{t("analysis.signalMetricTotal")}</span>
          <strong className={styles.metricValue}>{stats.total}</strong>
        </article>
        <article className={styles.metric}>
          <span className={styles.metricLabel}>{t("analysis.signalMetricPublic")}</span>
          <strong className={styles.metricValue}>{stats.publicSafe}</strong>
        </article>
        <article className={styles.metric}>
          <span className={styles.metricLabel}>{t("analysis.signalMetricEvidence")}</span>
          <strong className={styles.metricValue}>{stats.evidenceItems}</strong>
        </article>
        <article className={styles.metric}>
          <span className={styles.metricLabel}>{t("analysis.signalMetricSeverity")}</span>
          <strong className={styles.metricValue}>
            {stats.topSeverity ? t(`analysis.severity.${stats.topSeverity}`) : "—"}
          </strong>
        </article>
      </div>

      {signals ? (
        <div className={styles.runMeta}>
          <span>{t("analysis.signalLastRefresh")}: {formatDate(signals.last_refreshed_at)}</span>
          <span>{t("analysis.signalRunId")}: {signals.last_run_id ?? "—"}</span>
          <span>
            {signals.stale ? t("analysis.signalStale") : t("analysis.signalFresh")}
          </span>
        </div>
      ) : null}

      {error ? (
        <div className={styles.stateBlock}>
          <strong>{t("analysis.signalLoadError")}</strong>
          <p>{error}</p>
        </div>
      ) : null}

      {loading && !signals ? (
        <div className={styles.stateBlock}>
          <strong>{t("analysis.loadingSignals")}</strong>
          <p>{t("analysis.signalsLoadingLead")}</p>
        </div>
      ) : null}

      {!loading && signals && !signals.signals.length ? (
        <div className={styles.stateBlock}>
          <strong>{t("analysis.noMaterializedSignals")}</strong>
          <p>{t("analysis.signalsEmptyLead")}</p>
        </div>
      ) : null}

      {signals && signals.signals.length ? (
        <div className={styles.stream}>
          {signals.signals.map((signal) => (
            <article key={signal.hit_id} className={styles.signalArticle}>
              <div className={styles.signalHeader}>
                <div className={styles.signalHeaderMain}>
                  <div className={styles.signalMetaTop}>
                    <span className={`${styles.severityPill} ${severityClass(signal.severity)}`}>
                      {t(`analysis.severity.${signal.severity}`)}
                    </span>
                    <span className={styles.metaPill}>
                      {signal.public_safe ? t("analysis.signalPublicSafe") : t("analysis.signalReviewerOnly")}
                    </span>
                    <span className={styles.metaPill}>{signal.category}</span>
                  </div>
                  <div>
                    <div className={styles.headingRow}>
                      <h3 className={styles.signalTitle}>{signal.title}</h3>
                      <Link to={`/app/signals/${signal.signal_id}`} className={styles.signalLink}>
                        {signal.signal_id}
                      </Link>
                    </div>
                    <p className={styles.signalDescription}>{signal.description}</p>
                  </div>
                </div>
                <dl className={styles.signalStats}>
                  <div>
                    <dt>{t("analysis.signalMetricConfidence")}</dt>
                    <dd>{Math.round(signal.identity_confidence * 100)}%</dd>
                  </div>
                  <div>
                    <dt>{t("analysis.signalMetricScore")}</dt>
                    <dd>{signal.score.toFixed(1)}</dd>
                  </div>
                  <div>
                    <dt>{t("analysis.signalMetricEvidenceShort")}</dt>
                    <dd>{signal.evidence_count}</dd>
                  </div>
                </dl>
              </div>

              <div className={styles.signalMetaRow}>
                <span>{signal.entity_key}</span>
                {signal.scope_key ? <span>{signal.scope_key}</span> : null}
                <span>{signal.scope_type}</span>
                <span>{signal.identity_match_type ?? signal.identity_quality ?? "identity:unknown"}</span>
                {signal.run_id ? <span>{signal.run_id}</span> : null}
                <span>{signal.sources.length} {t("analysis.signalMetricSourcesShort")}</span>
              </div>

              {signal.evidence_items.length ? (
                <ul className={styles.evidenceList}>
                  {signal.evidence_items.slice(0, 4).map((item) => (
                    <li key={item.item_id} className={styles.evidenceItem}>
                      <div className={styles.evidenceLabelRow}>
                        {item.url ? (
                          <a href={item.url} target="_blank" rel="noreferrer" className={styles.evidenceLink}>
                            {item.label ?? item.url}
                          </a>
                        ) : (
                          <span className={styles.evidenceLink}>{item.label ?? item.record_id ?? item.item_id}</span>
                        )}
                        <span className={styles.sourceBadge}>{item.source_id ?? t("analysis.signalEvidence")}</span>
                        <span className={styles.sourceBadge}>{item.item_type}</span>
                        {item.identity_match_type ? (
                          <span className={styles.sourceBadge}>{item.identity_match_type}</span>
                        ) : null}
                        {item.node_ref ? (
                          <span className={styles.sourceBadge}>{item.node_ref}</span>
                        ) : null}
                      </div>
                      {item.observed_at ? (
                        <small className={styles.evidenceMeta}>
                          {new Date(item.observed_at).toLocaleString()}
                        </small>
                      ) : null}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className={styles.noEvidence}>{t("analysis.signalNoEvidenceItems")}</p>
              )}
            </article>
          ))}
        </div>
      ) : null}

      {signals && signals.signals.length ? (
        <footer className={styles.footerNote}>
          <span>{t("analysis.signalMetricReviewer")}: {stats.reviewerOnly}</span>
          <span>{t("analysis.signalMetricSources")}: {stats.sourceCount}</span>
          <span>{t("analysis.signalsFooterNote")}</span>
        </footer>
      ) : null}
    </section>
  );
}
