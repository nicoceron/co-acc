import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  ApiError,
  exportInvestigation,
  exportInvestigationPDF,
  generateShareLink,
  getCase,
  refreshCase,
  type CaseResponse,
} from "@/api/client";
import { useInvestigationStore } from "@/stores/investigation";

import styles from "./InvestigationDetail.module.css";

export function InvestigationDetail() {
  const { t, i18n } = useTranslation();
  const {
    investigations,
    activeInvestigationId,
    updateInvestigation,
    deleteInvestigation,
    addEntity,
    removeEntity,
    setActiveInvestigation,
  } = useInvestigationStore();

  const [entityInput, setEntityInput] = useState("");
  const [toast, setToast] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [caseData, setCaseData] = useState<CaseResponse | null>(null);
  const [caseLoading, setCaseLoading] = useState(false);
  const [caseRefreshing, setCaseRefreshing] = useState(false);
  const [caseError, setCaseError] = useState<string | null>(null);

  const investigation = useMemo(
    () => investigations.find((i) => i.id === activeInvestigationId),
    [investigations, activeInvestigationId],
  );

  const investigationId = investigation?.id;

  useEffect(() => {
    let cancelled = false;
    if (!investigationId) {
      setCaseData(null);
      setCaseError(null);
      setCaseLoading(false);
      return () => {
        cancelled = true;
      };
    }

    setCaseLoading(true);
    setCaseError(null);
    getCase(investigationId)
      .then((response) => {
        if (!cancelled) {
          setCaseData(response);
        }
      })
      .catch((loadError) => {
        if (cancelled) return;
        if (loadError instanceof ApiError && loadError.status === 404) {
          setCaseData(null);
          setCaseError(t("investigation.noSignals"));
          return;
        }
        setCaseData(null);
        setCaseError(loadError instanceof Error ? loadError.message : t("investigation.signalLoadError"));
      })
      .finally(() => {
        if (!cancelled) {
          setCaseLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [investigationId, t]);

  const showToast = useCallback((msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2000);
  }, []);

  const handleTitleBlur = useCallback(
    (e: React.FocusEvent<HTMLInputElement>) => {
      if (!investigation) return;
      const val = e.target.value.trim();
      if (val && val !== investigation.title) {
        updateInvestigation(investigation.id, { title: val });
      }
    },
    [investigation, updateInvestigation],
  );

  const handleDescBlur = useCallback(
    (e: React.FocusEvent<HTMLTextAreaElement>) => {
      if (!investigation) return;
      const val = e.target.value;
      if (val !== investigation.description) {
        updateInvestigation(investigation.id, { description: val });
      }
    },
    [investigation, updateInvestigation],
  );

  const handleAddEntity = useCallback(async () => {
    if (!investigation || !entityInput.trim()) return;
    await addEntity(investigation.id, entityInput.trim());
    setEntityInput("");
  }, [investigation, entityInput, addEntity]);

  const handleRemoveEntity = useCallback(
    async (entityId: string) => {
      if (!investigation) return;
      await removeEntity(investigation.id, entityId);
    },
    [investigation, removeEntity],
  );

  const handleShare = useCallback(async () => {
    if (!investigation) return;
    const { share_token } = await generateShareLink(investigation.id);
    const url = `${window.location.origin}/investigations/shared/${share_token}`;
    await navigator.clipboard.writeText(url);
    showToast(t("investigation.shareCopied"));
  }, [investigation, showToast, t]);

  const handleExport = useCallback(async () => {
    if (!investigation) return;
    const blob = await exportInvestigation(investigation.id);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${investigation.title}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [investigation]);

  const handleExportPDF = useCallback(async () => {
    if (!investigation) return;
    const lang = document.documentElement.lang === "en" ? "en" : "pt";
    const blob = await exportInvestigationPDF(investigation.id, lang);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${investigation.title}.pdf`;
    a.click();
    URL.revokeObjectURL(url);
  }, [investigation]);

  const handleDelete = useCallback(async () => {
    if (!investigation) return;
    if (!confirmDelete) {
      setConfirmDelete(true);
      return;
    }
    await deleteInvestigation(investigation.id);
    setActiveInvestigation(null);
    setConfirmDelete(false);
  }, [investigation, confirmDelete, deleteInvestigation, setActiveInvestigation]);

  const handleRefreshCase = useCallback(async () => {
    if (!investigation) return;
    setCaseRefreshing(true);
    setCaseError(null);
    try {
      const lang = i18n.resolvedLanguage?.startsWith("es") ? "es" : "en";
      const response = await refreshCase(investigation.id, lang);
      setCaseData(response);
    } catch (loadError) {
      setCaseError(loadError instanceof Error ? loadError.message : t("investigation.signalLoadError"));
    } finally {
      setCaseRefreshing(false);
    }
  }, [i18n.resolvedLanguage, investigation, t]);

  if (!investigation) {
    return <p className={styles.hint}>{t("investigation.noSelection")}</p>;
  }

  const signals = caseData?.signals ?? [];
  const events = caseData?.events.slice(0, 8) ?? [];

  return (
    <div className={styles.detail}>
      <div className={styles.titleRow}>
        <input
          className={styles.titleInput}
          defaultValue={investigation.title}
          onBlur={handleTitleBlur}
          key={investigation.id}
        />
      </div>

      <textarea
        className={styles.descInput}
        defaultValue={investigation.description}
        onBlur={handleDescBlur}
        placeholder={t("investigation.description")}
        key={`desc-${investigation.id}`}
      />

      <div className={styles.actions}>
        <button className={styles.actionButton} onClick={handleShare} type="button">
          {t("investigation.share")}
        </button>
        <button className={styles.actionButton} onClick={handleExport} type="button">
          {t("investigation.export")}
        </button>
        <button className={styles.actionButton} onClick={handleExportPDF} type="button">
          {t("investigation.exportPDF")}
        </button>
        <button className={styles.deleteButton} onClick={handleDelete} type="button">
          {confirmDelete ? t("investigation.deleteConfirm") : "X"}
        </button>
      </div>

      {toast && <span className={styles.toast}>{toast}</span>}

      <div className={styles.section}>
        <h3 className={styles.sectionTitle}>{t("investigation.entities")}</h3>
        <div className={styles.entityRow}>
          <input
            className={styles.entityInput}
            value={entityInput}
            onChange={(e) => setEntityInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") handleAddEntity(); }}
            placeholder={t("investigation.addEntity")}
          />
          <button className={styles.actionButton} onClick={handleAddEntity} type="button">
            +
          </button>
        </div>
        <div className={styles.entityList}>
          {investigation.entity_ids.map((eid) => (
            <span key={eid} className={styles.entityChip}>
              {eid}
              <button
                className={styles.removeButton}
                onClick={() => handleRemoveEntity(eid)}
                type="button"
                aria-label={t("investigation.removeEntity")}
              >
                x
              </button>
            </span>
          ))}
        </div>
      </div>

      <div className={styles.section}>
        <div className={styles.sectionHeader}>
          <h3 className={styles.sectionTitle}>{t("investigation.signals")}</h3>
          <div className={styles.sectionHeaderMeta}>
            {caseData ? (
              <span className={styles.counterPill}>
                {caseData.public_signal_count}/{caseData.signal_count} {t("investigation.publicSignals")}
              </span>
            ) : null}
            <button
              className={styles.actionButton}
              onClick={() => void handleRefreshCase()}
              type="button"
              disabled={caseRefreshing}
            >
              {caseRefreshing ? t("investigation.refreshingCase") : t("investigation.refreshCase")}
            </button>
          </div>
        </div>

        {caseData ? (
          <div className={styles.caseMeta}>
            <span>{caseData.stale ? t("investigation.caseStale") : t("investigation.caseFresh")}</span>
            <span>{t("investigation.caseRun")}: {caseData.last_run_id ?? "—"}</span>
            <span>
              {t("investigation.caseRefreshedAt")}: {caseData.last_refreshed_at ? new Date(caseData.last_refreshed_at).toLocaleString() : "—"}
            </span>
          </div>
        ) : null}

        {caseLoading ? (
          <p className={styles.hint}>{t("investigation.loadingSignals")}</p>
        ) : caseError ? (
          <p className={styles.error}>{caseError}</p>
        ) : signals.length === 0 ? (
          <p className={styles.hint}>{t("investigation.noSignals")}</p>
        ) : (
          <div className={styles.signalList}>
            {signals.map((signal) => (
              <article key={signal.hit_id} className={styles.signalCard}>
                <div className={styles.signalHeader}>
                  <div>
                    <div className={styles.signalMetaRow}>
                      <span className={`${styles.severityBadge} ${styles[`severity${signal.severity.charAt(0).toUpperCase()}${signal.severity.slice(1)}`]}`}>
                        {signal.severity}
                      </span>
                      <span className={styles.signalCategory}>{signal.category}</span>
                      <span className={styles.signalIdentity}>
                        {Math.round(signal.identity_confidence * 100)}% ID
                      </span>
                    </div>
                    <h4 className={styles.signalTitle}>{signal.title}</h4>
                  </div>
                  <div className={styles.signalMetrics}>
                    <strong>{signal.evidence_count}</strong>
                    <span>{t("investigation.evidenceItems")}</span>
                  </div>
                </div>
                <p className={styles.signalDescription}>{signal.description}</p>
                <div className={styles.signalContext}>
                  <span>{signal.entity_key}</span>
                  {signal.scope_key ? <span>{signal.scope_key}</span> : null}
                  <span>{signal.scope_type}</span>
                  <span>{signal.identity_match_type ?? signal.identity_quality ?? "identity:unknown"}</span>
                  {signal.run_id ? <span>{signal.run_id}</span> : null}
                  <span>{signal.public_safe ? t("investigation.publicSafe") : t("investigation.reviewerOnly")}</span>
                </div>
                {signal.evidence_items.length ? (
                  <ul className={styles.evidenceList}>
                    {signal.evidence_items.slice(0, 3).map((item) => (
                      <li key={item.item_id} className={styles.evidenceItem}>
                        {item.url ? (
                          <a href={item.url} target="_blank" rel="noreferrer" className={styles.evidenceLink}>
                            {item.label ?? item.url}
                          </a>
                        ) : (
                          <span className={styles.evidenceLink}>{item.label ?? item.record_id}</span>
                        )}
                        <small>
                          {[item.source_id ?? t("investigation.documentaryEvidence"), item.item_type, item.identity_match_type, item.node_ref]
                            .filter(Boolean)
                            .join(" · ")}
                        </small>
                      </li>
                    ))}
                  </ul>
                ) : null}
              </article>
            ))}
          </div>
        )}
      </div>

      {events.length ? (
        <div className={styles.section}>
          <h3 className={styles.sectionTitle}>{t("investigation.caseEvents")}</h3>
          <div className={styles.eventList}>
            {events.map((event) => (
              <div key={event.id} className={styles.eventRow}>
                <span className={styles.eventLabel}>{event.label}</span>
                <span className={styles.eventDate}>
                  {new Date(event.date).toLocaleString()}
                  {typeof event.bundle_document_count === "number" ? ` · ${event.bundle_document_count} docs` : ""}
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
