import { memo, useMemo } from "react";
import { useTranslation } from "react-i18next";

import type { EntityDetail, ExposureResponse, GraphData } from "@/api/client";
import { formatSourceName } from "@/lib/display";

import { ScoreRing } from "./ScoreRing";
import styles from "./CaseSummary.module.css";

interface CaseSummaryProps {
  entity: EntityDetail;
  graphData: GraphData | null;
  exposure: ExposureResponse | null;
  loading: boolean;
}

interface SignalCard {
  label: string;
  value: number;
}

function getEntityName(entity: EntityDetail): string {
  const rawName =
    entity.properties.name ??
    entity.properties.nome ??
    entity.properties.razon_social ??
    entity.id;
  return typeof rawName === "string" ? rawName : String(rawName);
}

function CaseSummaryInner({ entity, graphData, exposure, loading }: CaseSummaryProps) {
  const { t } = useTranslation();

  const name = getEntityName(entity);
  const connectedRecords = Math.max(0, (graphData?.nodes.length ?? 1) - 1);

  const graphMetrics = useMemo(() => {
    const nodeCounts = new Map<string, number>();
    const edgeCounts = new Map<string, number>();
    const sourceNames = new Set<string>();

    if (graphData) {
      for (const node of graphData.nodes) {
        nodeCounts.set(node.type, (nodeCounts.get(node.type) ?? 0) + 1);
        for (const source of node.sources) {
          sourceNames.add(source.database);
        }
      }
      for (const edge of graphData.edges) {
        edgeCounts.set(edge.type, (edgeCounts.get(edge.type) ?? 0) + 1);
        for (const source of edge.sources) {
          sourceNames.add(source.database);
        }
      }
    }

    return { nodeCounts, edgeCounts, sourceNames };
  }, [graphData]);

  const signalCards = useMemo<SignalCard[]>(() => {
    const cards: SignalCard[] = [
      {
        label: t("analysis.signalCampaignRecords"),
        value: graphMetrics.nodeCounts.get("election") ?? 0,
      },
      {
        label: t("analysis.signalDonationLinks"),
        value: graphMetrics.edgeCounts.get("DONO_A") ?? 0,
      },
      {
        label: t("analysis.signalPublicOffices"),
        value: graphMetrics.nodeCounts.get("publicOffice") ?? 0,
      },
      {
        label: t("analysis.signalPayrollLinks"),
        value: graphMetrics.edgeCounts.get("RECIBIO_SALARIO") ?? 0,
      },
      {
        label: t("analysis.signalAssetDisclosures"),
        value: graphMetrics.nodeCounts.get("declaredAsset") ?? 0,
      },
      {
        label: t("analysis.signalConflictDisclosures"),
        value: graphMetrics.nodeCounts.get("finance") ?? 0,
      },
      {
        label: t("analysis.signalConnectedCompanies"),
        value: graphMetrics.nodeCounts.get("company") ?? 0,
      },
      {
        label: t("analysis.signalContracts"),
        value: graphMetrics.nodeCounts.get("contract") ?? 0,
      },
      {
        label: t("analysis.signalSanctions"),
        value: graphMetrics.nodeCounts.get("sanction") ?? 0,
      },
    ];

    return cards.filter((card) => card.value > 0).slice(0, 6);
  }, [graphMetrics.edgeCounts, graphMetrics.nodeCounts, t]);

  const explanation = useMemo(() => {
    const fragments: string[] = [];

    if ((graphMetrics.edgeCounts.get("DONO_A") ?? 0) > 0) {
      fragments.push(t("analysis.summaryCampaignMoney"));
    }
    if ((graphMetrics.nodeCounts.get("publicOffice") ?? 0) > 0) {
      fragments.push(t("analysis.summaryPublicOffice"));
    }
    if ((graphMetrics.nodeCounts.get("declaredAsset") ?? 0) > 0 || (graphMetrics.nodeCounts.get("finance") ?? 0) > 0) {
      fragments.push(t("analysis.summaryDisclosures"));
    }
    if ((graphMetrics.nodeCounts.get("company") ?? 0) > 0 || (graphMetrics.nodeCounts.get("contract") ?? 0) > 0) {
      fragments.push(t("analysis.summaryContracts"));
    }

    return fragments;
  }, [graphMetrics.edgeCounts, graphMetrics.nodeCounts, t]);

  if (loading && !graphData) {
    return (
      <section className={styles.panel}>
        <div className={styles.header}>
          <div className={styles.intro}>
            <p className={styles.eyebrow}>{t("analysis.caseSummary")}</p>
            <h2 className={styles.title}>{t("analysis.caseSummaryTitle", { name })}</h2>
            <p className={styles.lead}>{t("common.loading")}</p>
          </div>
          {exposure && (
            <div className={styles.scoreCard}>
              <span className={styles.scoreLabel}>{t("analysis.exposureIndex")}</span>
              <ScoreRing value={exposure.exposure_index} size={72} />
            </div>
          )}
        </div>
      </section>
    );
  }

  return (
    <section className={styles.panel}>
      <div className={styles.header}>
        <div className={styles.intro}>
          <p className={styles.eyebrow}>{t("analysis.caseSummary")}</p>
          <h2 className={styles.title}>{t("analysis.caseSummaryTitle", { name })}</h2>
          <p className={styles.lead}>
            {t("analysis.caseSummaryLead", {
              records: connectedRecords,
              sources: graphMetrics.sourceNames.size,
            })}
          </p>
        </div>
        {exposure && (
          <div className={styles.scoreCard}>
            <span className={styles.scoreLabel}>{t("analysis.exposureIndex")}</span>
            <ScoreRing value={exposure.exposure_index} size={72} />
          </div>
        )}
      </div>

      {signalCards.length > 0 && (
        <div className={styles.section}>
          <p className={styles.sectionLabel}>{t("analysis.topSignals")}</p>
          <div className={styles.signalGrid}>
            {signalCards.map((card) => (
              <div key={card.label} className={styles.signalCard}>
                <strong className={styles.signalValue}>{card.value}</strong>
                <span className={styles.signalLabel}>{card.label}</span>
              </div>
            ))}
            <div className={styles.signalCard}>
              <strong className={styles.signalValue}>{connectedRecords}</strong>
              <span className={styles.signalLabel}>{t("analysis.signalRecords")}</span>
            </div>
            <div className={styles.signalCard}>
              <strong className={styles.signalValue}>{graphMetrics.sourceNames.size}</strong>
              <span className={styles.signalLabel}>{t("analysis.signalSources")}</span>
            </div>
          </div>
        </div>
      )}

      {explanation.length > 0 && (
        <div className={styles.section}>
          <p className={styles.sectionLabel}>{t("analysis.whatThisMeans")}</p>
          <div className={styles.explanationList}>
            {explanation.map((line) => (
              <p key={line} className={styles.explanationItem}>
                {line}
              </p>
            ))}
          </div>
        </div>
      )}

      <div className={styles.footer}>
        <div className={styles.readingGuide}>
          <p className={styles.sectionLabel}>{t("analysis.howToRead")}</p>
          <div className={styles.guideList}>
            <p>{t("analysis.howToReadStep1")}</p>
            <p>{t("analysis.howToReadStep2")}</p>
            <p>{t("analysis.howToReadStep3")}</p>
          </div>
        </div>
        <div className={styles.sourceBlock}>
          <p className={styles.sectionLabel}>{t("analysis.sourcesInView")}</p>
          <div className={styles.sourceRow}>
            {Array.from(graphMetrics.sourceNames).sort().slice(0, 8).map((sourceName) => (
              <span key={sourceName} className={styles.sourcePill}>
                {formatSourceName(sourceName)}
              </span>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

export const CaseSummary = memo(CaseSummaryInner);
