import { Search as SearchIcon, SlidersHorizontal } from "lucide-react";
import { FormEvent } from "react";
import { Link } from "react-router";

import { DataStatus, EmptyState, Frame, Pill, Rule } from "../components/ui";
import { allowAtlasFixtures, useAtlasSearch } from "../lib/useAtlasData";

const TYPES = [
  ["all", "todos"],
  ["empresa", "empresas"],
  ["persona", "personas"],
  ["contrato", "contratos"],
  ["sancion", "sanciones"],
] as const;

const ENTITY_TYPES = [
  ["empresa", "empresas"],
  ["persona", "personas"],
  ["contrato", "contratos"],
  ["sancion", "sanciones"],
] as const;

export function SearchPage() {
  const search = useAtlasSearch();
  const fixturesAllowed = allowAtlasFixtures();

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void search.runSearch();
  }

  function onTypeChange(nextType: string) {
    search.setType(nextType);
    void search.runSearch(search.query, nextType);
  }

  return (
    <main className="co-container co-search-page">
      <header className="co-workspace-head">
        <div>
          <Rule accent>Workspace · Busqueda</Rule>
          <h1>Buscar en el grafo</h1>
          <p>NIT, cedula, razon social, nombre o referencia documental.</p>
        </div>
        <DataStatus status={search.status} />
      </header>

      <form className="co-search-box" onSubmit={onSubmit}>
        <SearchIcon size={18} />
        <input
          value={search.query}
          onChange={(event) => search.setQuery(event.target.value)}
          placeholder="ej. NIT 900.412.118-5"
          autoFocus
        />
        <select value={search.type} onChange={(event) => onTypeChange(event.target.value)}>
          {TYPES.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select>
        <button className="co-button co-button--primary" type="submit">Buscar</button>
      </form>

      <div className="co-search-layout">
        <section>
          <div className="co-result-head">
            <span>{search.results.length} resultados · ordenado por score</span>
            <span>pagina 1/1</span>
          </div>
          <div className="co-table-wrap">
            <table className="co-table">
              <tbody>
                {search.results.map((result) => (
                  <tr key={result.id}>
                    <td><Pill>{result.type}</Pill></td>
                    <td>
                      <Link to={`/app/entity/${result.id}`}>{result.name}</Link>
                      <span className="co-row-sub">{result.doc} · {result.source}</span>
                    </td>
                    <td className="co-num">{result.score.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!search.results.length ? (
              <EmptyState title="Sin resultados" body="Ejecuta una busqueda contra la API para ver entidades materializadas." />
            ) : null}
          </div>
        </section>

        <aside className="co-search-filters">
          <Frame coord="FILTROS · F-01">
            <div className="co-frame-title">
              <div>
                <h2>Tipo de entidad</h2>
                <span>{fixturesAllowed ? "conteo de fixture local" : "conteo de resultados"}</span>
              </div>
              <SlidersHorizontal size={16} />
            </div>
            <div className="co-filter-list">
              {ENTITY_TYPES.map(([value, label]) => (
                <button
                  className={search.type === value ? "active" : ""}
                  key={value}
                  type="button"
                  onClick={() => onTypeChange(value)}
                >
                  <span>{label}</span>
                  <em>{search.counts[value]}</em>
                </button>
              ))}
            </div>
          </Frame>

          <Frame coord="FUENTES · F-02">
            {fixturesAllowed ? (
              <div className="co-chip-cloud">
                {["SECOP-II", "RUES", "SIRI", "SIGEP", "TVEC", "SGR", "ANLA", "CGR"].map((source) => (
                  <Pill key={source}>{source}</Pill>
                ))}
              </div>
            ) : (
              <EmptyState title="Fuentes segun resultados" body="Las fuentes se muestran en cada entidad devuelta por la API." />
            )}
          </Frame>
        </aside>
      </div>
    </main>
  );
}
