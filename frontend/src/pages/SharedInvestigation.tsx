import { useEffect, useState } from "react";
import { useParams } from "react-router";
import { useTranslation } from "react-i18next";
import { ArrowLeft, Share2 } from "lucide-react";
import { Link } from "react-router";

import type { Investigation } from "@/api/client";
import { getSharedInvestigation } from "@/api/client";

import styles from "./SharedInvestigation.module.css";

export function SharedInvestigation() {
  const { token } = useParams<{ token: string }>();
  const { t } = useTranslation();
  const [investigation, setInvestigation] = useState<Investigation | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!token) return;
    setLoading(true);
    getSharedInvestigation(token)
      .then(setInvestigation)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, [token]);

  if (loading) {
    return <p>{t("common.loading")}</p>;
  }

  if (error || !investigation) {
    return <p className={styles.error}>{t("investigation.sharedNotFound")}</p>;
  }

  return (
    <div className={styles.page}>
      <div className={styles.frame}>
        <div className={styles.topBar}>
          <Link to="/" className={styles.backLink}>
            <ArrowLeft size={16} />
            CO-ACC
          </Link>
          <span className={styles.badge}>
            <Share2 size={14} />
            {t("investigation.sharedView")}
          </span>
        </div>

        <div className={styles.header}>
          <h1 className={styles.title}>{investigation.title}</h1>
          {investigation.description && (
            <p className={styles.description}>{investigation.description}</p>
          )}
          <p className={styles.meta}>
            {t("investigation.created")}: {new Date(investigation.created_at).toLocaleString()}
          </p>
        </div>

        {investigation.entity_ids.length > 0 && (
          <div className={styles.section}>
            <h2 className={styles.sectionTitle}>{t("investigation.entities")}</h2>
            <p className={styles.sectionLead}>
              Esta vista compartida conserva la referencia de las entidades incluidas en la investigación original.
            </p>
            <div className={styles.entityList}>
              {investigation.entity_ids.map((eid) => (
                <span key={eid} className={styles.entityChip}>{eid}</span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
