import { useEffect, useMemo, useState } from "react";

import {
  getEntity,
  getEntityByElementId,
  getEntityEvidenceTrail,
  getEntitySignals,
  getEntityTimeline,
  getStats,
  listSources,
  listCases,
  listSignals,
  searchEntities,
  type CaseSummary,
  type EntityDetail,
  type EvidenceTrailBundle,
  type SearchResult,
  type SignalHit,
  type SignalListItem,
  type SourceRegistryItem,
  type StatsResponse,
  type TimelineEvent,
} from "@/api/client";

import {
  atlasFixture,
  type CaseStory,
  type Departamento,
  type FeedItem,
  type SectorItem,
  type SignalSummary,
  type SourceItem,
} from "../data/prototype";
import { searchIncludes } from "./format";

export interface OverviewMetrics {
  records: string;
  nodes: string;
  signals: string;
  sources: string;
  contracts: string;
  sanctions: string;
}

export interface AtlasOverview {
  metrics: OverviewMetrics;
  feed: FeedItem[];
  sources: SourceItem[];
  sectors: SectorItem[];
  departamentos: Departamento[];
  signals: SignalSummary[];
  cases: CaseStory[];
  seriesHits: number[];
  seriesSourcesOk: number[];
}

export interface OverviewState {
  data: AtlasOverview;
  status: AtlasDataStatus;
}

export interface AtlasSearchResult {
  id: string;
  type: string;
  name: string;
  doc: string;
  score: number;
  source: string;
}

export interface AtlasEntityState {
  status: AtlasDataStatus;
  entity?: EntityDetail;
  signals: SignalHit[];
  evidenceBundles: EvidenceTrailBundle[];
  timeline: TimelineEvent[];
  totalDocuments: number;
  error?: string;
}

export type AtlasDataStatus = "idle" | "loading" | "live" | "partial" | "fixture" | "unavailable";

const SECTOR_ACCENTS = [
  "var(--coacc-ochre)",
  "var(--coacc-coral)",
  "var(--coacc-cyan)",
  "var(--coacc-moss)",
  "var(--coacc-iris)",
  "var(--coacc-lime)",
  "var(--coacc-amber)",
  "var(--coacc-rose)",
];

export function allowAtlasFixtures(): boolean {
  const configured = import.meta.env.VITE_ALLOW_FIXTURES?.trim().toLowerCase();
  if (configured === "true") return true;
  if (configured === "false") return false;
  return import.meta.env.DEV;
}

function canFetch(): boolean {
  return typeof globalThis.fetch === "function";
}

function numericIdentifier(value: string): boolean {
  const clean = value.replace(/[.\-/]/g, "");
  return /^\d{5,14}$/.test(clean);
}

function loadEntity(identifier: string): Promise<EntityDetail> {
  if (numericIdentifier(identifier)) {
    return getEntity(identifier);
  }
  const clean = identifier.replace(/\D/g, "");
  return getEntityByElementId(identifier).catch((error: unknown) => {
    if (/^\d{5,14}$/.test(clean)) {
      return getEntity(clean);
    }
    throw error;
  });
}

function compactMetric(value: number | undefined, fallback: string): string {
  if (typeof value !== "number" || Number.isNaN(value)) return fallback;
  return new Intl.NumberFormat("es-CO", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

function emptyMetrics(): OverviewMetrics {
  return {
    records: "0",
    nodes: "0",
    signals: "0",
    sources: "0",
    contracts: "0",
    sanctions: "0",
  };
}

function buildFixtureOverview(): AtlasOverview {
  return {
    metrics: {
      records: "9.4M",
      nodes: "2.2M",
      signals: "43",
      sources: "16",
      contracts: "7.6M",
      sanctions: "221k",
    },
    feed: atlasFixture.feed,
    sources: atlasFixture.sources,
    sectors: atlasFixture.sectors,
    departamentos: atlasFixture.departamentos,
    signals: atlasFixture.signals,
    cases: atlasFixture.cases,
    seriesHits: atlasFixture.seriesHits,
    seriesSourcesOk: atlasFixture.seriesSourcesOk,
  };
}

function buildEmptyOverview(): AtlasOverview {
  return {
    metrics: emptyMetrics(),
    feed: [],
    sources: [],
    sectors: [],
    departamentos: [],
    signals: [],
    cases: [],
    seriesHits: [0, 0],
    seriesSourcesOk: [0, 0],
  };
}

function fallbackMetric(fixture: string, fixturesAllowed: boolean): string {
  return fixturesAllowed ? fixture : "0";
}

function mapStats(
  stats: StatsResponse | undefined,
  signalCount: number,
  sourceCount: number,
  fixturesAllowed: boolean,
): OverviewMetrics {
  return {
    records: compactMetric(stats?.source_document_count, fallbackMetric("9.4M", fixturesAllowed)),
    nodes: compactMetric(stats?.total_nodes, fallbackMetric("2.2M", fixturesAllowed)),
    signals: String(signalCount || (fixturesAllowed ? 43 : 0)),
    sources: String(stats?.data_sources ?? (sourceCount || (fixturesAllowed ? 16 : 0))),
    contracts: compactMetric(stats?.contract_count, fallbackMetric("7.6M", fixturesAllowed)),
    sanctions: compactMetric(stats?.sanction_count, fallbackMetric("221k", fixturesAllowed)),
  };
}

function mapSignal(item: SignalListItem): SignalSummary {
  return {
    id: item.id,
    severity: item.severity,
    title: item.title,
    desc: item.description,
    category: item.category,
    hits: item.hit_count,
    public: item.public_safe && !item.reviewer_only,
    lastSeen: item.last_seen_at,
    materialized: item.materialized,
    materializationState: item.materialization_state,
  };
}

function mapSource(item: SourceRegistryItem): SourceItem {
  const loaded = item.load_state === "loaded";
  const implemented = item.implementation_state === "implemented";
  const cov = loaded ? 1 : implemented ? 0.58 : 0.18;
  return {
    code: item.id,
    name: item.name || item.id,
    desc: [item.category, item.tier, item.status].filter(Boolean).join(" · "),
    cov,
    rows: item.load_state.replace(/_/g, " "),
    stale: item.status === "stale" || item.quality_status === "stale",
  };
}

function feedFromSignals(signals: SignalSummary[]): FeedItem[] {
  return signals
    .filter((signal) => signal.hits > 0)
    .sort((a, b) => b.hits - a.hits)
    .slice(0, 12)
    .map((signal) => ({
      t: signal.lastSeen?.slice(11, 19) || "lake",
      signal: signal.id,
      severity: signal.severity,
      entity: signal.title,
      doc: signal.category,
      note: `${compactMetric(signal.hits, "0")} hits materializados`,
    }));
}

function sectorsFromSignals(signals: SignalSummary[]): SectorItem[] {
  const byCategory = new Map<string, { hits: number; count: number }>();
  for (const signal of signals) {
    const current = byCategory.get(signal.category) ?? { hits: 0, count: 0 };
    byCategory.set(signal.category, {
      hits: current.hits + signal.hits,
      count: current.count + 1,
    });
  }
  return Array.from(byCategory.entries())
    .sort((a, b) => b[1].hits - a[1].hits)
    .map(([category, value], index) => ({
      id: category,
      label: category,
      hits: value.hits,
      contracts: `${value.count} senales`,
      accent: SECTOR_ACCENTS[index % SECTOR_ACCENTS.length] ?? "var(--coacc-accent)",
    }));
}

function seriesFromValues(values: number[]): number[] {
  const clean = values.filter((value) => Number.isFinite(value) && value >= 0);
  if (!clean.length) return [0, 0];
  if (clean.length === 1) return [0, clean[0] ?? 0];
  return clean.slice(0, 30).reverse();
}

function mapCase(item: CaseSummary, index: number): CaseStory {
  return {
    slug: item.id,
    date: item.last_refreshed_at?.slice(0, 10) || item.updated_at?.slice(0, 10) || item.created_at.slice(0, 10),
    kicker: item.status === "published" ? "Investigacion publicada" : "Caso de trabajo",
    title: item.title,
    lede: item.description || "Dossier construido con señales materializadas, entidades asociadas y evidencia primaria.",
    docs: Math.max(12, item.signal_count * 8 + index * 3),
    entities: item.entity_ids.length,
    signals: item.signal_count ? [`${item.signal_count} senales`] : ["sin senales"],
    published: item.status === "published" || item.public_signal_count > 0,
  };
}

function fixtureSearch(query: string, type: string): AtlasSearchResult[] {
  const normalized = query.trim();
  return atlasFixture.feed
    .map((item, index) => ({
      id: `fixture-${index}`,
      type: ["empresa", "persona", "contrato", "sancion"][index % 4] ?? "empresa",
      name: item.entity,
      doc: item.doc,
      score: 0.94 - index * 0.05,
      source: atlasFixture.sources[index % atlasFixture.sources.length]?.code ?? "SECOP-II",
    }))
    .filter((result) => type === "all" || result.type === type)
    .filter((result) => !normalized || searchIncludes(`${result.name} ${result.doc} ${result.source}`, normalized))
    .slice(0, 8);
}

function mapSearchResult(item: SearchResult): AtlasSearchResult {
  return {
    id: item.id,
    type: item.type || "entidad",
    name: item.name,
    doc: item.document || "sin documento",
    score: item.score,
    source: item.sources[0]?.database || "co/acc",
  };
}

export function useAtlasOverview(): OverviewState {
  const fixturesAllowed = allowAtlasFixtures();
  const [state, setState] = useState<OverviewState>({
    data: fixturesAllowed ? buildFixtureOverview() : buildEmptyOverview(),
    status: "loading",
  });

  useEffect(() => {
    let active = true;
    if (!canFetch()) {
      setState({
        data: fixturesAllowed ? buildFixtureOverview() : buildEmptyOverview(),
        status: fixturesAllowed ? "fixture" : "unavailable",
      });
      return () => {
        active = false;
      };
    }

    void Promise.allSettled([getStats(), listSignals(), listCases(1, 8), listSources()]).then((results) => {
      if (!active) return;

      const statsResult = results[0];
      const signalsResult = results[1];
      const casesResult = results[2];
      const sourcesResult = results[3];
      const stats = statsResult.status === "fulfilled" ? statsResult.value : undefined;
      const signals = signalsResult.status === "fulfilled"
        ? signalsResult.value.signals.map(mapSignal)
        : fixturesAllowed ? atlasFixture.signals : [];
      const cases = casesResult.status === "fulfilled"
        ? casesResult.value.cases.map(mapCase)
        : fixturesAllowed ? atlasFixture.cases : [];
      const sources = sourcesResult.status === "fulfilled"
        ? sourcesResult.value.sources.map(mapSource)
        : fixturesAllowed ? atlasFixture.sources : [];
      const liveCount = results.filter((result) => result.status === "fulfilled").length;
      const feed = signalsResult.status === "fulfilled"
        ? feedFromSignals(signals)
        : fixturesAllowed ? atlasFixture.feed : [];
      const sectors = signalsResult.status === "fulfilled"
        ? sectorsFromSignals(signals)
        : fixturesAllowed ? atlasFixture.sectors : [];

      setState({
        data: {
          metrics: mapStats(stats, signals.length, sources.length, fixturesAllowed),
          feed,
          sources,
          sectors,
          departamentos: fixturesAllowed ? atlasFixture.departamentos : [],
          signals,
          cases,
          seriesHits: signalsResult.status === "fulfilled"
            ? seriesFromValues(signals.map((signal) => signal.hits))
            : fixturesAllowed ? atlasFixture.seriesHits : [0, 0],
          seriesSourcesOk: sourcesResult.status === "fulfilled"
            ? seriesFromValues(sources.map((source) => Math.round(source.cov * 100)))
            : fixturesAllowed ? atlasFixture.seriesSourcesOk : [0, 0],
        },
        status: liveCount === results.length
          ? "live"
          : liveCount > 0
            ? "partial"
            : fixturesAllowed ? "fixture" : "unavailable",
      });
    });

    return () => {
      active = false;
    };
  }, [fixturesAllowed]);

  return state;
}

function emptyTypeCounts(): Record<"empresa" | "persona" | "contrato" | "sancion", number> {
  return {
    empresa: 0,
    persona: 0,
    contrato: 0,
    sancion: 0,
  };
}

function resultTypeCounts(results: AtlasSearchResult[]): Record<"empresa" | "persona" | "contrato" | "sancion", number> {
  const counts = emptyTypeCounts();
  for (const result of results) {
    if (result.type in counts) {
      counts[result.type as keyof typeof counts] += 1;
    }
  }
  return counts;
}

export function useAtlasSearch(initialQuery?: string) {
  const fixturesAllowed = allowAtlasFixtures();
  const startingQuery = initialQuery ?? (fixturesAllowed ? "vertice andina" : "");
  const [query, setQuery] = useState(startingQuery);
  const [type, setType] = useState("all");
  const [results, setResults] = useState<AtlasSearchResult[]>(() => (
    fixturesAllowed ? fixtureSearch(startingQuery, "all") : []
  ));
  const [status, setStatus] = useState<AtlasDataStatus>(fixturesAllowed ? "fixture" : "idle");

  const counts = useMemo(() => {
    if (!fixturesAllowed) return resultTypeCounts(results);
    return {
      empresa: fixtureSearch("", "empresa").length,
      persona: fixtureSearch("", "persona").length,
      contrato: fixtureSearch("", "contrato").length,
      sancion: fixtureSearch("", "sancion").length,
    };
  }, [fixturesAllowed, results]);

  async function runSearch(nextQuery = query, nextType = type): Promise<void> {
    setStatus("loading");
    const trimmed = nextQuery.trim();
    if (!trimmed) {
      setResults(fixturesAllowed ? fixtureSearch(nextQuery, nextType) : []);
      setStatus(fixturesAllowed ? "fixture" : "idle");
      return;
    }
    if (!canFetch()) {
      setResults(fixturesAllowed ? fixtureSearch(nextQuery, nextType) : []);
      setStatus(fixturesAllowed ? "fixture" : "unavailable");
      return;
    }

    try {
      const response = await searchEntities(trimmed, nextType, 1, 12);
      setResults(response.results.map(mapSearchResult));
      setStatus("live");
    } catch {
      setResults(fixturesAllowed ? fixtureSearch(nextQuery, nextType) : []);
      setStatus(fixturesAllowed ? "fixture" : "unavailable");
    }
  }

  return {
    query,
    setQuery,
    type,
    setType,
    results,
    status,
    counts,
    runSearch,
  };
}

export function useAtlasEntity(entityId: string | undefined): AtlasEntityState {
  const fixturesAllowed = allowAtlasFixtures();
  const [state, setState] = useState<AtlasEntityState>({
    status: entityId ? "loading" : fixturesAllowed ? "fixture" : "idle",
    signals: [],
    evidenceBundles: [],
    timeline: [],
    totalDocuments: 0,
  });

  useEffect(() => {
    let active = true;
    const requestedId = entityId?.trim();

    if (!requestedId) {
      setState({
        status: fixturesAllowed ? "fixture" : "idle",
        signals: [],
        evidenceBundles: [],
        timeline: [],
        totalDocuments: 0,
      });
      return () => {
        active = false;
      };
    }

    if (!canFetch()) {
      setState({
        status: fixturesAllowed ? "fixture" : "unavailable",
        signals: [],
        evidenceBundles: [],
        timeline: [],
        totalDocuments: 0,
        error: "fetch unavailable",
      });
      return () => {
        active = false;
      };
    }

    setState((current) => ({
      ...current,
      status: "loading",
      error: undefined,
    }));

    void loadEntity(requestedId)
      .then(async (entity) => {
        const liveId = entity.id || requestedId;
        const [signalsResult, evidenceResult, timelineResult] = await Promise.allSettled([
          getEntitySignals(liveId),
          getEntityEvidenceTrail(liveId, 8),
          getEntityTimeline(liveId, undefined, 12),
        ]);

        if (!active) return;

        const signals = signalsResult.status === "fulfilled" ? signalsResult.value.signals : [];
        const evidenceBundles = evidenceResult.status === "fulfilled" ? evidenceResult.value.bundles : [];
        const timeline = timelineResult.status === "fulfilled" ? timelineResult.value.events : [];
        const totalDocuments = evidenceResult.status === "fulfilled" ? evidenceResult.value.total_documents : 0;
        const enrichmentResults = [signalsResult, evidenceResult, timelineResult];
        const enrichmentFailures = enrichmentResults.filter((result) => result.status === "rejected").length;

        setState({
          status: enrichmentFailures ? "partial" : "live",
          entity,
          signals,
          evidenceBundles,
          timeline,
          totalDocuments,
        });
      })
      .catch((error: unknown) => {
        if (!active) return;
        setState({
          status: fixturesAllowed ? "fixture" : "unavailable",
          signals: [],
          evidenceBundles: [],
          timeline: [],
          totalDocuments: 0,
          error: error instanceof Error ? error.message : "Entity lookup failed",
        });
      });

    return () => {
      active = false;
    };
  }, [entityId, fixturesAllowed]);

  return state;
}
