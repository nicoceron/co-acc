import { NavLink, Outlet } from "react-router";
import { clsx } from "clsx";

import { IS_PATTERNS_ENABLED } from "@/config/runtime";

const NAV = [
  { to: "/app", label: "Overview", end: true },
  { to: "/app/search", label: "Search" },
  { to: "/app/signals", label: "Signals" },
  { to: "/app/cases", label: "Cases" },
  ...(IS_PATTERNS_ENABLED ? [{ to: "/app/patterns", label: "Patterns" }] : []),
];

export function AppShell() {
  return (
    <div className="grid min-h-screen grid-cols-1 md:grid-cols-[220px_1fr] bg-ink-950">
      <aside className="sticky top-0 hidden h-screen flex-col border-r border-white/5 px-4 py-5 md:flex">
        <NavLink to="/" className="px-2 pb-6 font-mono text-sm tracking-tight text-ink-50">
          <span className="text-lime-300">co</span>
          <span className="text-ink-500">/</span>
          <span>acc</span>
        </NavLink>

        <nav className="flex flex-col gap-0.5">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                clsx(
                  "rounded-md px-2.5 py-1.5 text-[13px] text-ink-400 transition hover:bg-white/[0.03] hover:text-ink-50",
                  isActive && "bg-white/[0.04] text-ink-50",
                )
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className="mt-auto flex items-center gap-2 border-t border-white/5 px-2 pt-4">
          <span className="h-1.5 w-1.5 rounded-full bg-lime-400" />
          <span className="font-mono text-[11px] text-ink-400">public_safe</span>
        </div>
      </aside>

      <header className="flex h-14 items-center justify-between border-b border-white/5 px-4 md:hidden">
        <NavLink to="/" className="font-mono text-sm text-ink-50">
          <span className="text-lime-300">co</span>/<span>acc</span>
        </NavLink>
        <nav className="flex gap-3 text-[12px] text-ink-400">
          {NAV.map((item) => (
            <NavLink key={item.to} to={item.to} end={item.end}>
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>

      <main className="min-w-0">
        <Outlet />
      </main>
    </div>
  );
}
