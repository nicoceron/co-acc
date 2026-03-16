import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { type EntityDetail as EntityDetailData, getEntity, getEntityByElementId } from "@/api/client";
import { SourceBadge } from "@/components/common/SourceBadge";
import { formatPropertyLabel } from "@/lib/display";
import { type EntityType, entityColors } from "@/styles/tokens";

import styles from "./EntityDetail.module.css";

interface EntityDetailProps {
  entityId: string | null;
  onClose: () => void;
}

const DOCUMENT_KEYS = [
  "document_id",
  "nit",
  "cedula",
  "numero_documento",
  "cpf",
  "cnpj",
  "bid_id",
  "asset_id",
  "finance_id",
  "office_id",
] as const;

function cleanIdentifier(value: string): string {
  return value.replace(/[.\-/]/g, "");
}

function resolveDocumentValue(
  properties: EntityDetailData["properties"],
): string | null {
  for (const key of DOCUMENT_KEYS) {
    const value = properties[key];
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return null;
}

export function EntityDetail({ entityId, onClose }: EntityDetailProps) {
  const { t } = useTranslation();
  const [entity, setEntity] = useState<EntityDetailData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!entityId) {
      setEntity(null);
      return;
    }
    setLoading(true);
    const cleaned = cleanIdentifier(entityId);
    const isDocumentIdentifier = /^\d{5,14}$/.test(cleaned);
    const fetcher = isDocumentIdentifier ? getEntity(cleaned) : getEntityByElementId(entityId);
    fetcher
      .then(setEntity)
      .catch(() => setEntity(null))
      .finally(() => setLoading(false));
  }, [entityId]);

  if (!entityId) return null;

  const documentValue = entity ? resolveDocumentValue(entity.properties) : null;

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <span className={styles.title}>{t("entity.detail")}</span>
        <button onClick={onClose} className={styles.close}>
          &times;
        </button>
      </div>

      {loading && <p className={styles.loading}>{t("common.loading")}</p>}

      {entity && (
        <div className={styles.content}>
          <div
            className={styles.typeTag}
            style={{ color: entityColors[entity.type as EntityType] ?? "#555" }}
          >
            {t(`entity.${entity.type}`, entity.type)}
          </div>
          <h3 className={styles.name}>
            {String(entity.properties.name ?? entity.properties.razon_social ?? entity.properties.nome ?? "N/A")}
          </h3>

          {documentValue && (
            <p className={styles.document}>
              {documentValue}
            </p>
          )}

          <div className={styles.properties}>
            {Object.entries(entity.properties).filter(
              ([key]) => !["name", "razon_social", "nome", ...DOCUMENT_KEYS].includes(key),
            ).map(([key, value]) => (
              <div key={key} className={styles.property}>
                <span className={styles.propKey}>{formatPropertyLabel(key)}</span>
                <span className={styles.propValue}>{String(value ?? "—")}</span>
              </div>
            ))}
          </div>

          {entity.sources.length > 0 && (
            <div className={styles.sources}>
              <span className={styles.sourcesLabel}>{t("common.source")}:</span>
              {entity.sources.map((s) => (
                <SourceBadge key={s.database} source={s.database} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
