import { memo } from "react";
import { useTranslation } from "react-i18next";
import { Network, Users, Clock, Download, Radar } from "lucide-react";

import styles from "./AnalysisNav.module.css";

type AnalysisTab = "graph" | "connections" | "timeline" | "signals" | "export";

interface AnalysisNavProps {
  activeTab: AnalysisTab;
  onTabChange: (tab: AnalysisTab) => void;
}

const TABS: { id: AnalysisTab; icon: typeof Network; labelKey: string }[] = [
  { id: "connections", icon: Users, labelKey: "analysis.summary" },
  { id: "graph", icon: Network, labelKey: "analysis.graph" },
  { id: "timeline", icon: Clock, labelKey: "analysis.timeline" },
  { id: "signals", icon: Radar, labelKey: "analysis.signals" },
  { id: "export", icon: Download, labelKey: "analysis.export" },
];

function AnalysisNavInner({ activeTab, onTabChange }: AnalysisNavProps) {
  const { t } = useTranslation();

  return (
    <nav className={styles.nav} aria-label={t("analysis.navigation")}>
      {TABS.map(({ id, icon: Icon, labelKey }) => (
        <button
          key={id}
          className={`${styles.btn} ${activeTab === id ? styles.active : ""}`}
          onClick={() => onTabChange(id)}
          title={t(labelKey)}
          aria-label={t(labelKey)}
          aria-current={activeTab === id ? "page" : undefined}
        >
          <Icon size={18} />
          <span className={styles.label}>{t(labelKey)}</span>
        </button>
      ))}
    </nav>
  );
}

export const AnalysisNav = memo(AnalysisNavInner);
