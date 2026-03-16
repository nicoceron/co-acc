import { memo } from "react";
import { useTranslation } from "react-i18next";
import { ArrowLeft, Plus } from "lucide-react";

import type { EntityDetail, ExposureResponse } from "@/api/client";
import { formatSourceName } from "@/lib/display";
import { entityColors } from "@/styles/tokens";

import { ScoreRing } from "./ScoreRing";
import styles from "./EntityHeader.module.css";

interface EntityHeaderProps {
  entity: EntityDetail;
  exposure: ExposureResponse | null;
  graphStats?: {
    connectionCount: number;
    sourceNames: string[];
  };
  onBack: () => void;
  onAddToInvestigation: () => void;
}

function formatMoney(value: number): string {
  return new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    notation: "compact",
    maximumFractionDigits: 0,
  }).format(value);
}

function EntityHeaderInner({
  entity,
  exposure,
  graphStats,
  onBack,
  onAddToInvestigation,
}: EntityHeaderProps) {
  const { t } = useTranslation();

  const rawName =
    entity.properties.nome ??
    entity.properties.razon_social ??
    entity.properties.name ??
    entity.id;
  const name = typeof rawName === "string" ? rawName : String(rawName);

  const typeColor = entityColors[entity.type] ?? "var(--text-muted)";

  const connectionCount =
    graphStats?.connectionCount ??
    exposure?.factors.find((f) => f.name === "connections")?.value;
  const sourceNames = graphStats?.sourceNames ?? entity.sources.map((source) => source.database);
  const sourceCount = sourceNames.length;
  const totalMoney = exposure?.factors.find((f) => f.name === "financial");

  return (
    <header className={styles.header}>
      <button
        className={styles.backBtn}
        onClick={onBack}
        aria-label={t("common.back")}
      >
        <ArrowLeft size={16} />
      </button>

      <span className={styles.name}>{name}</span>

      <span className={styles.typeBadge}>
        <span
          className={styles.typeDot}
          style={{ backgroundColor: typeColor }}
        />
        {t(`entity.${entity.type}`, entity.type)}
      </span>

      {exposure && (
        <ScoreRing value={exposure.exposure_index} size={40} />
      )}

      <div className={styles.sourceBadges}>
        {sourceNames.slice(0, 4).map((sourceName) => (
          <span key={sourceName} className={styles.sourcePill}>
            {formatSourceName(sourceName)}
          </span>
        ))}
        {sourceNames.length > 4 && (
          <span className={styles.sourcePill}>+{sourceNames.length - 4}</span>
        )}
      </div>

      <div className={styles.stats}>
        {connectionCount != null && (
          <span className={styles.stat}>
            {connectionCount} {t("common.connections")}
          </span>
        )}
        <span className={styles.stat}>
          {sourceCount} {t("common.sources")}
        </span>
        {totalMoney && (
          <span className={styles.stat}>{formatMoney(totalMoney.value)}</span>
        )}
      </div>

      <button className={styles.addBtn} onClick={onAddToInvestigation}>
        <Plus size={14} />
        {t("investigation.addEntity")}
      </button>
    </header>
  );
}

export const EntityHeader = memo(EntityHeaderInner);
