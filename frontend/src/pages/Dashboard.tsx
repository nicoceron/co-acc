import { type FormEvent, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Link, useNavigate } from "react-router";
import { AlertTriangle } from "lucide-react";

import {
  getPrioritizedBuyers,
  getPrioritizedCompanies,
  getPrioritizedPeople,
  getPrioritizedTerritories,
  listInvestigations,
  searchEntities,
  type Investigation,
  type RiskAlert,
  type SearchResult,
  type PrioritizedBuyer,
  type PrioritizedCompany,
  type PrioritizedPerson,
  type PrioritizedTerritory,
} from "@/api/client";
import { Skeleton } from "@/components/common/Skeleton";
import { useToastStore } from "@/stores/toast";

import styles from "./Dashboard.module.css";

type PeopleFilterId =
  | "all"
  | "office_contract"
  | "donor_contract"
  | "disclosure_contract"
  | "candidates";

const PEOPLE_WATCHLIST_LIMIT = 24;
const COMPANY_WATCHLIST_LIMIT = 24;
const BUYER_WATCHLIST_LIMIT = 12;
const TERRITORY_WATCHLIST_LIMIT = 12;
const QUICK_SEARCH_LIMIT = 12;

const PEOPLE_FILTERS: Array<{
  id: PeopleFilterId;
  labelKey: string;
  matches: (person: PrioritizedPerson) => boolean;
}> = [
  {
    id: "all",
    labelKey: "dashboard.peopleFilterAll",
    matches: () => true,
  },
  {
    id: "office_contract",
    labelKey: "dashboard.peopleFilterOfficeSupplier",
    matches: (person) => person.office_count > 0 && person.supplier_contract_count > 0,
  },
  {
    id: "donor_contract",
    labelKey: "dashboard.peopleFilterDonorVendor",
    matches: (person) => person.donation_count > 0 && person.supplier_contract_count > 0,
  },
  {
    id: "disclosure_contract",
    labelKey: "dashboard.peopleFilterDisclosures",
    matches: (person) => (
      person.supplier_contract_count > 0
      && (
        person.conflict_disclosure_count > 0
        || person.disclosure_reference_count > 0
        || person.corporate_activity_disclosure_count > 0
      )
    ),
  },
  {
    id: "candidates",
    labelKey: "dashboard.peopleFilterCandidates",
    matches: (person) => person.candidacy_count > 0,
  },
];

function getNumberLocale(language: string): string {
  if (language.startsWith("es")) {
    return "es-CO";
  }
  if (language === "es-CO") {
    return "es-CO";
  }
  return "en-US";
}

function formatCompactCurrency(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "COP",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatRatio(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "0x";
  }
  if (value >= 1000) {
    return `${Math.round(value).toLocaleString("en-US")}x`;
  }
  if (value >= 10) {
    return `${value.toFixed(0)}x`;
  }
  return `${value.toFixed(1)}x`;
}

function formatPercent(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(value);
}

function getPersonContext(person: PrioritizedPerson, t: (key: string) => string): string {
  if (person.alerts[0]?.reason_text) {
    return person.alerts[0].reason_text;
  }
  if (person.donor_vendor_loop_count > 0) {
    return t("dashboard.donorVendorLoopLead");
  }
  if (person.disclosure_reference_count > 0) {
    return t("dashboard.disclosureReferenceLead");
  }
  if (person.corporate_activity_disclosure_count > 0) {
    return t("dashboard.corporateActivityLead");
  }
  if (person.supplier_contract_count > 0) {
    return t("dashboard.vendorContractLead");
  }
  return person.offices[0] || t("dashboard.watchlistFallbackRole");
}

function getCompanyContext(company: PrioritizedCompany, t: (key: string) => string): string {
  if (company.alerts[0]?.reason_text) {
    return company.alerts[0].reason_text;
  }
  const capacityRatio = Math.max(
    company.capacity_mismatch_revenue_ratio,
    company.capacity_mismatch_asset_ratio,
  );
  if (capacityRatio >= 2) {
    return t("dashboard.capacityMismatchLead");
  }
  if (company.funding_overlap_event_count > 0) {
    return t("dashboard.publicFundingLead");
  }
  return company.official_names[0] || t("dashboard.companyFallbackRole");
}

function getBuyerContext(buyer: PrioritizedBuyer, t: (key: string) => string): string {
  return buyer.alerts[0]?.reason_text ?? t("dashboard.buyerFallbackLead");
}

function getTerritoryContext(
  territory: PrioritizedTerritory,
  t: (key: string) => string,
): string {
  return territory.alerts[0]?.reason_text ?? t("dashboard.territoryFallbackLead");
}

function getConfidenceLabel(alert: RiskAlert, t: (key: string) => string): string {
  if (alert.confidence_tier === "A") {
    return t("dashboard.alertExact");
  }
  if (alert.confidence_tier === "B") {
    return t("dashboard.alertAggregate");
  }
  return t("dashboard.alertInference");
}

function getFindingClassLabel(alert: RiskAlert, t: (key: string) => string): string {
  return t(`dashboard.findingClass.${alert.finding_class}`);
}

function AlertSummary({
  alerts,
  t,
}: {
  alerts: RiskAlert[];
  t: (key: string) => string;
}) {
  if (alerts.length === 0) {
    return null;
  }

  return (
    <div className={styles.alertStack}>
      {alerts.slice(0, 2).map((alert) => (
        <div key={`${alert.alert_type}-${alert.confidence_tier}`} className={styles.alertCard}>
          <div className={styles.alertHeader}>
            <span className={styles.alertBadge}>
              {getConfidenceLabel(alert, t)}
            </span>
            <span className={styles.alertBadgeMuted}>
              {getFindingClassLabel(alert, t)}
            </span>
          </div>
          <p className={styles.alertReason}>{alert.reason_text}</p>
          <p className={styles.alertMeta}>
            {t("dashboard.alertSources")}: {alert.source_list.join(" · ")}
          </p>
          {alert.what_is_unproven && (
            <p className={styles.alertCaveat}>
              {t("dashboard.alertUnproven")}: {alert.what_is_unproven}
            </p>
          )}
          {alert.next_step && (
            <p className={styles.alertNextStep}>
              {t("dashboard.alertNextStep")}: {alert.next_step}
            </p>
          )}
        </div>
      ))}
    </div>
  );
}

export function Dashboard() {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const addToast = useToastStore((s) => s.addToast);

  const [recentInvestigations, setRecentInvestigations] = useState<Investigation[]>([]);
  const [loadingInvestigations, setLoadingInvestigations] = useState(true);
  const [watchlist, setWatchlist] = useState<PrioritizedPerson[]>([]);
  const [loadingWatchlist, setLoadingWatchlist] = useState(true);
  const [watchlistError, setWatchlistError] = useState(false);
  const [companyWatchlist, setCompanyWatchlist] = useState<PrioritizedCompany[]>([]);
  const [loadingCompanyWatchlist, setLoadingCompanyWatchlist] = useState(true);
  const [companyWatchlistError, setCompanyWatchlistError] = useState(false);
  const [buyerWatchlist, setBuyerWatchlist] = useState<PrioritizedBuyer[]>([]);
  const [loadingBuyerWatchlist, setLoadingBuyerWatchlist] = useState(true);
  const [buyerWatchlistError, setBuyerWatchlistError] = useState(false);
  const [territoryWatchlist, setTerritoryWatchlist] = useState<PrioritizedTerritory[]>([]);
  const [loadingTerritoryWatchlist, setLoadingTerritoryWatchlist] = useState(true);
  const [territoryWatchlistError, setTerritoryWatchlistError] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [activePeopleFilter, setActivePeopleFilter] = useState<PeopleFilterId>("all");

  useEffect(() => {
    listInvestigations(1, 3)
      .then((res) => setRecentInvestigations(res.investigations))
      .catch(() => {})
      .finally(() => setLoadingInvestigations(false));
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadWatchlists() {
      const [
        peopleResult,
        companyResult,
        buyerResult,
        territoryResult,
      ] = await Promise.allSettled([
        getPrioritizedPeople(PEOPLE_WATCHLIST_LIMIT),
        getPrioritizedCompanies(COMPANY_WATCHLIST_LIMIT),
        getPrioritizedBuyers(BUYER_WATCHLIST_LIMIT),
        getPrioritizedTerritories(TERRITORY_WATCHLIST_LIMIT),
      ]);

      if (cancelled) {
        return;
      }

      if (peopleResult.status === "fulfilled") {
        setWatchlist(peopleResult.value.people);
        setWatchlistError(false);
      } else {
        setWatchlist([]);
        setWatchlistError(true);
      }
      setLoadingWatchlist(false);

      if (companyResult.status === "fulfilled") {
        setCompanyWatchlist(companyResult.value.companies);
        setCompanyWatchlistError(false);
      } else {
        setCompanyWatchlist([]);
        setCompanyWatchlistError(true);
      }
      setLoadingCompanyWatchlist(false);

      if (buyerResult.status === "fulfilled") {
        setBuyerWatchlist(buyerResult.value.buyers);
        setBuyerWatchlistError(false);
      } else {
        setBuyerWatchlist([]);
        setBuyerWatchlistError(true);
      }
      setLoadingBuyerWatchlist(false);

      if (territoryResult.status === "fulfilled") {
        setTerritoryWatchlist(territoryResult.value.territories);
        setTerritoryWatchlistError(false);
      } else {
        setTerritoryWatchlist([]);
        setTerritoryWatchlistError(true);
      }
      setLoadingTerritoryWatchlist(false);
    }

    void loadWatchlists();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleSearch = async (e: FormEvent) => {
    e.preventDefault();
    const q = searchQuery.trim();
    if (!q) return;
    setSearching(true);
    try {
      const res = await searchEntities(q, undefined, 1, QUICK_SEARCH_LIMIT);
      setSearchResults(res.results);
    } catch {
      setSearchResults([]);
      addToast("error", t("search.error"));
    } finally {
      setSearching(false);
    }
  };

  const flaggedCompanyValue = companyWatchlist.reduce(
    (sum, company) => sum + company.contract_value,
    0,
  );
  const numberLocale = getNumberLocale(i18n.language);
  const numberFormatter = new Intl.NumberFormat(numberLocale);
  const filteredWatchlist = useMemo(() => {
    const activeFilter = PEOPLE_FILTERS.find((filter) => filter.id === activePeopleFilter);
    if (!activeFilter) {
      return watchlist;
    }
    return watchlist.filter(activeFilter.matches);
  }, [activePeopleFilter, watchlist]);

  return (
    <div className={styles.page}>
      <section className={styles.hero}>
        <div className={styles.heroCopy}>
          <p className={styles.kicker}>{t("dashboard.watchlistKicker")}</p>
          <h1 className={styles.title}>{t("dashboard.welcome")}</h1>
          <p className={styles.subtitle}>{t("dashboard.watchlistSubtitle")}</p>
        </div>

        <div className={styles.heroStats}>
          <div className={styles.heroStat}>
            <span className={styles.heroStatLabel}>{t("dashboard.peopleFlagged")}</span>
            <strong className={styles.heroStatValue}>
              {loadingWatchlist ? "..." : numberFormatter.format(watchlist.length)}
            </strong>
          </div>
          <div className={styles.heroStat}>
            <span className={styles.heroStatLabel}>{t("dashboard.companiesFlagged")}</span>
            <strong className={styles.heroStatValue}>
              {loadingCompanyWatchlist ? "..." : numberFormatter.format(companyWatchlist.length)}
            </strong>
          </div>
          <div className={styles.heroStat}>
            <span className={styles.heroStatLabel}>{t("dashboard.buyersFlagged")}</span>
            <strong className={styles.heroStatValue}>
              {loadingBuyerWatchlist ? "..." : numberFormatter.format(buyerWatchlist.length)}
            </strong>
          </div>
          <div className={styles.heroStat}>
            <span className={styles.heroStatLabel}>{t("dashboard.territoriesFlagged")}</span>
            <strong className={styles.heroStatValue}>
              {loadingTerritoryWatchlist ? "..." : numberFormatter.format(territoryWatchlist.length)}
            </strong>
          </div>
          <div className={styles.heroStat}>
            <span className={styles.heroStatLabel}>{t("dashboard.exposedContractValue")}</span>
            <strong className={styles.heroStatValue}>
              {loadingCompanyWatchlist ? "..." : formatCompactCurrency(flaggedCompanyValue, numberLocale)}
            </strong>
          </div>
        </div>
      </section>

      <section className={styles.watchlistSection}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>{t("dashboard.watchlistLabel")}</p>
            <h2 className={styles.sectionTitle}>{t("dashboard.prioritizedPeople")}</h2>
          </div>
          <p className={styles.sectionNote}>{t("dashboard.watchlistNote")}</p>
        </div>

        {!loadingWatchlist && !watchlistError && watchlist.length > 0 && (
          <div className={styles.filterBar}>
            <span className={styles.filterLabel}>{t("dashboard.peopleFiltersLabel")}</span>
            <div className={styles.filterChips}>
              {PEOPLE_FILTERS.map((filter) => (
                <button
                  key={filter.id}
                  type="button"
                  className={`${styles.filterChip} ${activePeopleFilter === filter.id ? styles.filterChipActive : ""}`}
                  onClick={() => setActivePeopleFilter(filter.id)}
                  aria-pressed={activePeopleFilter === filter.id}
                >
                  {t(filter.labelKey)}
                </button>
              ))}
            </div>
            <span className={styles.filterMeta}>
              {t("dashboard.peopleFilterCount", {
                count: filteredWatchlist.length,
                total: watchlist.length,
              })}
            </span>
          </div>
        )}

        {loadingWatchlist ? (
          <div className={styles.watchlistGrid}>
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
          </div>
        ) : watchlistError ? (
          <p className={styles.empty}>{t("dashboard.watchlistError")}</p>
        ) : watchlist.length === 0 ? (
          <p className={styles.empty}>{t("dashboard.watchlistEmpty")}</p>
        ) : filteredWatchlist.length === 0 ? (
          <p className={styles.empty}>{t("dashboard.peopleFilterEmpty")}</p>
        ) : (
          <div className={styles.watchlistGrid}>
            {filteredWatchlist.map((person, index) => (
              <button
                key={person.entity_id}
                className={styles.watchlistCard}
                onClick={() => navigate(`/app/analysis/${encodeURIComponent(person.entity_id)}`)}
              >
                <div className={styles.cardTop}>
                  <span className={styles.rank}>#{index + 1}</span>
                  <span className={styles.scoreBadge}>
                    <AlertTriangle size={14} />
                    {t("dashboard.scoreLabel")} {numberFormatter.format(person.suspicion_score)}
                  </span>
                </div>

                <div className={styles.personHeader}>
                  <h3 className={styles.personName}>{person.name}</h3>
                  {person.document_id && (
                    <p className={styles.personDoc}>{person.document_id}</p>
                  )}
                </div>

                <div className={styles.officeStrip}>
                  <span>{getPersonContext(person, t)}</span>
                </div>

                <div className={styles.metricGrid}>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.publicRoles")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.office_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.donations")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.donation_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.vendorContracts")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.supplier_contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.conflictFlags")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.conflict_disclosure_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.disclosureRefs")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.disclosure_reference_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.candidacies")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(person.candidacy_count)}
                    </strong>
                  </div>
                </div>

                <AlertSummary alerts={person.alerts} t={t} />

                <div className={styles.cardFooter}>
                  <div>
                    <p className={styles.footerLabel}>
                      {person.supplier_contract_count > 0
                        ? t("dashboard.contractExposure")
                        : t("dashboard.donationValue")}
                    </p>
                    <strong className={styles.footerValue}>
                      {formatCompactCurrency(
                        person.supplier_contract_count > 0
                          ? person.supplier_contract_value
                          : person.donation_value,
                        numberLocale,
                      )}
                    </strong>
                  </div>
                  <span className={styles.openAnalysis}>{t("dashboard.openAnalysis")}</span>
                </div>
              </button>
            ))}
          </div>
        )}
      </section>

      <section className={styles.watchlistSection}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>{t("dashboard.watchlistLabel")}</p>
            <h2 className={styles.sectionTitle}>{t("dashboard.prioritizedCompanies")}</h2>
          </div>
          <p className={styles.sectionNote}>{t("dashboard.companyWatchlistSubtitle")}</p>
        </div>

        {loadingCompanyWatchlist ? (
          <div className={styles.watchlistGrid}>
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
          </div>
        ) : companyWatchlistError ? (
          <p className={styles.empty}>{t("dashboard.companyWatchlistError")}</p>
        ) : companyWatchlist.length === 0 ? (
          <p className={styles.empty}>{t("dashboard.companyWatchlistEmpty")}</p>
        ) : (
          <div className={styles.watchlistGrid}>
            {companyWatchlist.map((company, index) => (
              <button
                key={company.entity_id}
                className={styles.watchlistCard}
                onClick={() => navigate(`/app/analysis/${encodeURIComponent(company.entity_id)}`)}
              >
                <div className={styles.cardTop}>
                  <span className={styles.rank}>#{index + 1}</span>
                  <span className={styles.scoreBadge}>
                    <AlertTriangle size={14} />
                    {t("dashboard.scoreLabel")} {numberFormatter.format(company.suspicion_score)}
                  </span>
                </div>

                <div className={styles.personHeader}>
                  <h3 className={styles.personName}>{company.name}</h3>
                  {company.document_id && (
                    <p className={styles.personDoc}>{company.document_id}</p>
                  )}
                </div>

                <div className={styles.officeStrip}>
                  <span>{getCompanyContext(company, t)}</span>
                </div>

                <div className={styles.metricGrid}>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.publicFundingOverlap")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(company.funding_overlap_event_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.capacityMismatch")}</span>
                    <strong className={styles.metricValue}>
                      {formatRatio(
                        Math.max(
                          company.capacity_mismatch_revenue_ratio,
                          company.capacity_mismatch_asset_ratio,
                        ),
                      )}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.lowCompetition")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(company.low_competition_bid_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.sanctions")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(company.sanction_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.executionGap")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(company.execution_gap_contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.flaggedContracts")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(company.contract_count)}
                    </strong>
                  </div>
                </div>

                <AlertSummary alerts={company.alerts} t={t} />

                <div className={styles.cardFooter}>
                  <div>
                    <p className={styles.footerLabel}>{t("dashboard.contractExposure")}</p>
                    <strong className={styles.footerValue}>
                      {formatCompactCurrency(company.contract_value, numberLocale)}
                    </strong>
                  </div>
                  <span className={styles.openAnalysis}>{t("dashboard.openAnalysis")}</span>
                </div>
              </button>
            ))}
          </div>
        )}
      </section>

      <section className={styles.watchlistSection}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>{t("dashboard.watchlistLabel")}</p>
            <h2 className={styles.sectionTitle}>{t("dashboard.prioritizedBuyers")}</h2>
          </div>
          <p className={styles.sectionNote}>{t("dashboard.buyerWatchlistSubtitle")}</p>
        </div>

        {loadingBuyerWatchlist ? (
          <div className={styles.watchlistGrid}>
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
          </div>
        ) : buyerWatchlistError ? (
          <p className={styles.empty}>{t("dashboard.buyerWatchlistError")}</p>
        ) : buyerWatchlist.length === 0 ? (
          <p className={styles.empty}>{t("dashboard.buyerWatchlistEmpty")}</p>
        ) : (
          <div className={styles.watchlistGrid}>
            {buyerWatchlist.map((buyer, index) => (
              <article key={buyer.buyer_id} className={`${styles.watchlistCard} ${styles.staticCard}`}>
                <div className={styles.cardTop}>
                  <span className={styles.rank}>#{index + 1}</span>
                  <span className={styles.scoreBadge}>
                    <AlertTriangle size={14} />
                    {t("dashboard.scoreLabel")} {numberFormatter.format(buyer.suspicion_score)}
                  </span>
                </div>

                <div className={styles.personHeader}>
                  <h3 className={styles.personName}>{buyer.buyer_name}</h3>
                  {buyer.buyer_document_id && (
                    <p className={styles.personDoc}>{buyer.buyer_document_id}</p>
                  )}
                </div>

                <div className={styles.officeStrip}>
                  <span>{getBuyerContext(buyer, t)}</span>
                </div>

                <div className={styles.metricGrid}>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.flaggedContracts")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(buyer.contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.suppliers")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(buyer.supplier_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.topSupplierShare")}</span>
                    <strong className={styles.metricValue}>
                      {formatPercent(buyer.top_supplier_share, numberLocale)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.sanctions")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(buyer.sanctioned_supplier_contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.executionGap")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(buyer.discrepancy_contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.privateInterests")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(buyer.official_overlap_supplier_count)}
                    </strong>
                  </div>
                </div>

                <AlertSummary alerts={buyer.alerts} t={t} />

                <div className={styles.cardFooter}>
                  <div>
                    <p className={styles.footerLabel}>{t("dashboard.contractExposure")}</p>
                    <strong className={styles.footerValue}>
                      {formatCompactCurrency(buyer.contract_value, numberLocale)}
                    </strong>
                  </div>
                  <span className={styles.openAnalysis}>{t("dashboard.manualReview")}</span>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <section className={styles.watchlistSection}>
        <div className={styles.sectionHeader}>
          <div>
            <p className={styles.sectionEyebrow}>{t("dashboard.watchlistLabel")}</p>
            <h2 className={styles.sectionTitle}>{t("dashboard.prioritizedTerritories")}</h2>
          </div>
          <p className={styles.sectionNote}>{t("dashboard.territoryWatchlistSubtitle")}</p>
        </div>

        {loadingTerritoryWatchlist ? (
          <div className={styles.watchlistGrid}>
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
            <Skeleton variant="rect" height="320px" />
          </div>
        ) : territoryWatchlistError ? (
          <p className={styles.empty}>{t("dashboard.territoryWatchlistError")}</p>
        ) : territoryWatchlist.length === 0 ? (
          <p className={styles.empty}>{t("dashboard.territoryWatchlistEmpty")}</p>
        ) : (
          <div className={styles.watchlistGrid}>
            {territoryWatchlist.map((territory, index) => (
              <article
                key={territory.territory_id}
                className={`${styles.watchlistCard} ${styles.staticCard}`}
              >
                <div className={styles.cardTop}>
                  <span className={styles.rank}>#{index + 1}</span>
                  <span className={styles.scoreBadge}>
                    <AlertTriangle size={14} />
                    {t("dashboard.scoreLabel")} {numberFormatter.format(territory.suspicion_score)}
                  </span>
                </div>

                <div className={styles.personHeader}>
                  <h3 className={styles.personName}>{territory.territory_name}</h3>
                  <p className={styles.personDoc}>{territory.department}</p>
                </div>

                <div className={styles.officeStrip}>
                  <span>{getTerritoryContext(territory, t)}</span>
                </div>

                <div className={styles.metricGrid}>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.flaggedContracts")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(territory.contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.buyers")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(territory.buyer_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.suppliers")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(territory.supplier_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.topSupplierShare")}</span>
                    <strong className={styles.metricValue}>
                      {formatPercent(territory.top_supplier_share, numberLocale)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.sanctions")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(territory.sanctioned_supplier_contract_count)}
                    </strong>
                  </div>
                  <div className={styles.metric}>
                    <span className={styles.metricLabel}>{t("dashboard.executionGap")}</span>
                    <strong className={styles.metricValue}>
                      {numberFormatter.format(territory.discrepancy_contract_count)}
                    </strong>
                  </div>
                </div>

                <AlertSummary alerts={territory.alerts} t={t} />

                <div className={styles.cardFooter}>
                  <div>
                    <p className={styles.footerLabel}>{t("dashboard.contractExposure")}</p>
                    <strong className={styles.footerValue}>
                      {formatCompactCurrency(territory.contract_value, numberLocale)}
                    </strong>
                  </div>
                  <span className={styles.openAnalysis}>{t("dashboard.manualReview")}</span>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <div className={styles.lowerGrid}>
        <section className={styles.searchSection}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.sectionEyebrow}>{t("dashboard.quickSearch")}</p>
              <h2 className={styles.sectionTitle}>{t("dashboard.quickSearch")}</h2>
            </div>
          </div>
          <form className={styles.searchForm} onSubmit={handleSearch}>
            <input
              type="text"
              className={styles.searchInput}
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={t("search.placeholder")}
            />
            <button type="submit" className={styles.searchBtn} disabled={searching}>
              {t("search.button")}
            </button>
          </form>
          {searchResults.length > 0 && (
            <ul className={styles.quickResults}>
              {searchResults.map((r) => (
                <li key={r.id}>
                  <Link to={`/app/analysis/${r.id}`} className={styles.quickResultLink}>
                    <span className={styles.quickResultType}>
                      {t(`entity.${r.type}`, r.type)}
                    </span>
                    <span className={styles.quickResultName}>{r.name}</span>
                    {r.document && (
                      <span className={styles.quickResultDoc}>{r.document}</span>
                    )}
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className={styles.investigationsSection}>
          <div className={styles.sectionHeader}>
            <div>
              <p className={styles.sectionEyebrow}>{t("dashboard.recentInvestigations")}</p>
              <h2 className={styles.sectionTitle}>{t("dashboard.recentInvestigations")}</h2>
            </div>
          </div>
          {loadingInvestigations ? (
            <div className={styles.skeletons}>
              <Skeleton variant="rect" height="72px" />
              <Skeleton variant="rect" height="72px" />
              <Skeleton variant="rect" height="72px" />
            </div>
          ) : recentInvestigations.length === 0 ? (
            <p className={styles.empty}>{t("dashboard.noRecent")}</p>
          ) : (
            <div className={styles.investigationCards}>
              {recentInvestigations.map((inv) => (
                <button
                  key={inv.id}
                  className={styles.investigationCard}
                  onClick={() => navigate(`/app/investigations/${inv.id}`)}
                >
                  <span className={styles.invTitle}>{inv.title}</span>
                  <span className={styles.invMeta}>
                    {inv.entity_ids.length} {t("common.connections")} &middot;{" "}
                    {new Date(inv.updated_at).toLocaleDateString(numberLocale)}
                  </span>
                </button>
              ))}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
