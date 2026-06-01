import { useEffect, useMemo, useState } from "react";

import {
  getStats,
  listCases,
  listSignals,
  searchEntities,
  type CaseSummary,
  type SearchResult,
  type SignalListItem,
  type StatsResponse,
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
  status: "loading" | "live" | "fixture";
}

export interface AtlasSearchResult {
  id: string;
  type: string;
  name: string;
  doc: string;
  score: number;
  source: string;
}

function canFetch(): boolean {
  return typeof globalThis.fetch === "function";
}

function compactMetric(value: number | undefined, fallback: string): string {
  if (typeof value !== "number" || Number.isNaN(value)) return fallback;
  return new Intl.NumberFormat("es-CO", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
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

function mapStats(stats: StatsResponse | undefined, signalCount: number): OverviewMetrics {
  return {
    records: compactMetric(stats?.source_document_count, "9.4M"),
    nodes: compactMetric(stats?.total_nodes, "2.2M"),
    signals: String(signalCount || 43),
    sources: String(stats?.data_sources ?? 16),
    contracts: compactMetric(stats?.contract_count, "7.6M"),
    sanctions: compactMetric(stats?.sanction_count, "221k"),
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
  };
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
  const [state, setState] = useState<OverviewState>({
    data: buildFixtureOverview(),
    status: "loading",
  });

  useEffect(() => {
    let active = true;
    if (!canFetch()) {
      setState({ data: buildFixtureOverview(), status: "fixture" });
      return () => {
        active = false;
      };
    }

    void Promise.allSettled([getStats(), listSignals(), listCases(1, 8)]).then((results) => {
      if (!active) return;

      const statsResult = results[0];
      const signalsResult = results[1];
      const casesResult = results[2];
      const stats = statsResult.status === "fulfilled" ? statsResult.value : undefined;
      const signals = signalsResult.status === "fulfilled"
        ? signalsResult.value.signals.map(mapSignal)
        : atlasFixture.signals;
      const cases = casesResult.status === "fulfilled"
        ? casesResult.value.cases.map(mapCase)
        : atlasFixture.cases;
      const hasLiveData = results.some((result) => result.status === "fulfilled");

      setState({
        data: {
          metrics: mapStats(stats, signals.length),
          feed: atlasFixture.feed,
          sources: atlasFixture.sources,
          sectors: atlasFixture.sectors,
          departamentos: atlasFixture.departamentos,
          signals,
          cases,
          seriesHits: atlasFixture.seriesHits,
          seriesSourcesOk: atlasFixture.seriesSourcesOk,
        },
        status: hasLiveData ? "live" : "fixture",
      });
    });

    return () => {
      active = false;
    };
  }, []);

  return state;
}

export function useAtlasSearch(initialQuery = "vertice andina") {
  const [query, setQuery] = useState(initialQuery);
  const [type, setType] = useState("all");
  const [results, setResults] = useState<AtlasSearchResult[]>(() => fixtureSearch(initialQuery, "all"));
  const [status, setStatus] = useState<"idle" | "loading" | "live" | "fixture">("fixture");

  const counts = useMemo(() => {
    return {
      empresa: fixtureSearch("", "empresa").length,
      persona: fixtureSearch("", "persona").length,
      contrato: fixtureSearch("", "contrato").length,
      sancion: fixtureSearch("", "sancion").length,
    };
  }, []);

  async function runSearch(nextQuery = query, nextType = type): Promise<void> {
    setStatus("loading");
    const trimmed = nextQuery.trim();
    if (!canFetch() || !trimmed) {
      setResults(fixtureSearch(nextQuery, nextType));
      setStatus("fixture");
      return;
    }

    try {
      const response = await searchEntities(trimmed, nextType, 1, 12);
      setResults(response.results.map(mapSearchResult));
      setStatus("live");
    } catch {
      setResults(fixtureSearch(nextQuery, nextType));
      setStatus("fixture");
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
