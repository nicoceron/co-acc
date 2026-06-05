import { ArrowLeft, ArrowRight, FileText, Network, ShieldCheck } from "lucide-react";
import { Link, useParams } from "react-router";

import { EmptyState, Eyebrow, Frame, Pill, Rule, SeverityBadge } from "../components/ui";
import { atlasFixture, type CaseStory } from "../data/prototype";
import { allowAtlasFixtures, useAtlasOverview } from "../lib/useAtlasData";

function CaseStats({ story }: { story: CaseStory }) {
  return (
    <div className="co-case-stats">
      <span><strong>{story.docs}</strong> documentos</span>
      <span><strong>{story.entities}</strong> entidades</span>
      <span><strong>{story.signals.length}</strong> senales</span>
    </div>
  );
}

function SignalList({ story }: { story: CaseStory }) {
  return (
    <div className="co-signal-list">
      {story.signals.map((signal) => {
        const detail = atlasFixture.signals.find((item) => item.id === signal);
        return detail ? <SeverityBadge key={signal} severity={detail.severity} /> : <Pill key={signal}>{signal}</Pill>;
      })}
    </div>
  );
}

export function Cases() {
  const { data } = useAtlasOverview();
  const fixturesAllowed = allowAtlasFixtures();
  const featured = data.cases[0] ?? (fixturesAllowed ? atlasFixture.cases[0] : undefined);
  const rest = data.cases.slice(1);

  return (
    <main className="co-container co-cases">
      <Rule accent>Casos publicados · dossier editorial</Rule>
      <header className="co-page-head co-page-head--wide">
        <h1>Lo que el grafo ayuda a contar.</h1>
        <p>Investigaciones documentales armadas desde senales cruzadas y evidencia primaria.</p>
      </header>

      {featured ? (
        <div className="co-cases__grid">
          <Frame coord="DESTACADO · D-01" className="co-featured-case">
            <Eyebrow accent>{featured.kicker} · {featured.date}</Eyebrow>
            <h2>{featured.title}</h2>
            <p>{featured.lede}</p>
            <CaseStats story={featured} />
            <SignalList story={featured} />
            <Link className="co-button co-button--primary" to={`/casos/${featured.slug}`}>
              Abrir dossier
              <ArrowRight size={16} />
            </Link>
          </Frame>

          <aside className="co-case-index">
            {rest.map((story) => (
              <Link className="co-case-row" key={story.slug} to={`/casos/${story.slug}`}>
                <span>{story.date}</span>
                <strong>{story.title}</strong>
                <em>{story.kicker}</em>
              </Link>
            ))}
          </aside>
        </div>
      ) : (
        <EmptyState title="Sin casos publicados" body="La API no entrego casos para esta superficie publica." />
      )}
    </main>
  );
}

export function CaseDetail() {
  const { slug } = useParams();
  const { data } = useAtlasOverview();
  const fixturesAllowed = allowAtlasFixtures();
  const story = data.cases.find((item) => item.slug === slug)
    ?? (fixturesAllowed ? atlasFixture.cases.find((item) => item.slug === slug) : undefined)
    ?? (fixturesAllowed ? atlasFixture.cases[0] : undefined);

  if (!story) {
    return (
      <main className="co-container co-case-detail">
        <Link className="co-button co-button--ghost" to="/casos">
          <ArrowLeft size={16} />
          Casos
        </Link>
        <EmptyState title="Dossier no disponible" body="No hay caso materializado con ese identificador." />
      </main>
    );
  }

  return (
    <main className="co-container co-case-detail">
      <Link className="co-button co-button--ghost" to="/casos">
        <ArrowLeft size={16} />
        Casos
      </Link>
      <header className="co-page-head">
        <Eyebrow accent>{story.kicker} · {story.date}</Eyebrow>
        <h1>{story.title}</h1>
        <p>{story.lede}</p>
        <CaseStats story={story} />
      </header>

      <div className="co-case-detail__grid">
        <Frame coord="EVIDENCIA · E-01">
          <h2>Paquete documental</h2>
          <div className="co-evidence-list">
            {[
              ["Contrato principal", "SECOP-II", "proceso CO1.PCCNTR.4421"],
              ["Registro empresarial", "RUES", "representacion legal y camara"],
              ["Sancion activa", "SIRI", "ventana temporal de adjudicacion"],
              ["Relacion contractual", "TVEC", "orden y modificatorios"],
            ].map(([title, source, note]) => (
              <article key={title}>
                <FileText size={17} />
                <div>
                  <strong>{title}</strong>
                  <span>{source} · {note}</span>
                </div>
              </article>
            ))}
          </div>
        </Frame>

        <Frame coord="LECTURA · R-02">
          <h2>Contexto de lectura</h2>
          <div className="co-dossier-notes">
            <p>El dossier presenta coincidencias documentales y relaciones temporales. No sustituye investigacion judicial ni afirma responsabilidad individual.</p>
            <p>Las entidades quedan enlazadas por identificadores publicos, proceso contractual, fuente y fecha de observacion.</p>
          </div>
          <div className="co-inline-metrics">
            <span><Network size={16} /> {story.entities} entidades</span>
            <span><ShieldCheck size={16} /> public_safe</span>
          </div>
          <SignalList story={story} />
        </Frame>
      </div>
    </main>
  );
}
