import { NavLink, Outlet } from "react-router";
import { clsx } from "clsx";

const NAV = [
  { to: "/casos", label: "Casos" },
  { to: "/sector", label: "Sectores" },
  { to: "/app", label: "Workspace" },
];

export function PublicShell() {
  return (
    <div className="min-h-screen bg-ink-950">
      <header className="sticky top-0 z-40 border-b border-white/5 bg-ink-950/80 backdrop-blur-xl">
        <div className="mx-auto flex h-14 max-w-[1400px] items-center justify-between px-6 md:px-10">
          <NavLink to="/" className="font-mono text-sm tracking-tight text-ink-50">
            <span className="text-lime-300">co</span>
            <span className="text-ink-500">/</span>
            <span>acc</span>
          </NavLink>
          <nav className="flex items-center gap-1">
            {NAV.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  clsx(
                    "rounded-md px-3 py-1.5 text-[13px] text-ink-400 transition hover:text-ink-50",
                    isActive && "text-ink-50",
                  )
                }
              >
                {item.label}
              </NavLink>
            ))}
            <a
              href="https://github.com/nicoceron/co-acc"
              target="_blank"
              rel="noreferrer"
              className="rounded-md px-3 py-1.5 text-[13px] text-ink-400 transition hover:text-ink-50"
            >
              GitHub ↗
            </a>
          </nav>
        </div>
      </header>
      <main>
        <Outlet />
      </main>
    </div>
  );
}
