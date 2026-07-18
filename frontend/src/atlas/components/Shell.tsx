import { Menu, Radio, Search, X } from "lucide-react";
import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "react-router";

import { Brand, Pill } from "./ui";

const NAV_ITEMS = [
  { label: "Patrones", to: "/app/patterns" },
  { label: "Señales", to: "/app/signals" },
  { label: "Buscar", to: "/app/search" },
  { label: "Priorizados", to: "/priorizados" },
  { label: "Metodología", to: "/metodologia" },
];

function isActive(pathname: string, target: string): boolean {
  if (target === "/") return pathname === "/";
  return pathname === target || pathname.startsWith(`${target}/`);
}

export function AtlasShell() {
  const { pathname } = useLocation();
  const [navOpen, setNavOpen] = useState(false);
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 30_000);
    return () => window.clearInterval(timer);
  }, []);

  const hh = String(now.getHours()).padStart(2, "0");
  const mm = String(now.getMinutes()).padStart(2, "0");

  return (
    <>
      <header className="co-topbar">
        <Link to="/" className="co-topbar__brand" onClick={() => setNavOpen(false)}>
          <Brand />
        </Link>

        <nav className={navOpen ? "co-topbar__nav co-topbar__nav--open" : "co-topbar__nav"} aria-label="Principal">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className={isActive(pathname, item.to) ? "active" : ""}
              onClick={() => setNavOpen(false)}
            >
              {item.label}
            </Link>
          ))}
        </nav>

        <div className="co-topbar__right">
          <span className="co-topbar__coord">04N · 74W</span>
          <span className="co-divider" />
          <span className="co-mono">BOG · {hh}:{mm}</span>
          <Pill tone="moss">
            <Radio size={12} />
            live
          </Pill>
          <Link className="co-icon-button co-topbar__search" to="/app/search" aria-label="Buscar">
            <Search size={16} />
          </Link>
          <button
            className="co-icon-button co-topbar__menu"
            type="button"
            aria-label={navOpen ? "Cerrar menu" : "Abrir menu"}
            onClick={() => setNavOpen((open) => !open)}
          >
            {navOpen ? <X size={16} /> : <Menu size={16} />}
          </button>
        </div>
      </header>

      <div className="co-page-grid">
        <Outlet />
      </div>

      <footer className="co-footer">
        <div className="co-container co-footer__grid">
          <div>
            <Brand />
            <p>Datos públicos de Colombia. Contexto documental, no acusación.</p>
          </div>
          <div>
            <strong>Producto</strong>
            <Link to="/app/patterns">Patrones</Link>
            <Link to="/app/signals">Señales</Link>
            <Link to="/app/search">Buscar</Link>
            <Link to="/priorizados">Contratos priorizados</Link>
          </div>
          <div>
            <strong>Metodologia</strong>
            <Link to="/metodologia">Cómo funciona</Link>
            <a href="https://github.com/nicoceron/co-acc/blob/main/DISCLAIMER.md">Aviso legal</a>
            <a href="https://github.com/nicoceron/co-acc/blob/main/PRIVACY.md">Privacidad</a>
            <a href="https://github.com/nicoceron/co-acc/blob/main/ETHICS.md">Etica</a>
            <a href="https://github.com/nicoceron/co-acc/blob/main/SECURITY.md">Seguridad</a>
          </div>
          <div>
            <strong>Repositorio</strong>
            <a href="https://github.com/nicoceron/co-acc">GitHub</a>
            <a href="https://github.com/nicoceron/co-acc/issues">Issues</a>
            <span className="co-mono">AGPL-3.0</span>
            <span className="co-mono">confidence indexed</span>
          </div>
        </div>
        <div className="co-container co-footer__bar">
          <span>2026 · co/acc</span>
          <span>confidence_index · AGPL-3.0 · atlas v3</span>
        </div>
      </footer>
    </>
  );
}
