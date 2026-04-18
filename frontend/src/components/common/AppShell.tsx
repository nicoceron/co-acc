import { NavLink, Outlet } from "react-router";

import { IS_PATTERNS_ENABLED } from "@/config/runtime";

import styles from "./AppShell.module.css";

const NAV = [
  { to: "/app", label: "Overview", end: true },
  { to: "/app/search", label: "Search" },
  { to: "/app/signals", label: "Signals" },
  { to: "/app/cases", label: "Cases" },
  ...(IS_PATTERNS_ENABLED ? [{ to: "/app/patterns", label: "Patterns" }] : []),
];

export function AppShell() {
  return (
    <div className={styles.shell}>
      <aside className={styles.sidebar}>
        <NavLink to="/" className={styles.brand}>
          <span className={styles.brandMark}>co/</span>
          <span className={styles.brandName}>acc</span>
        </NavLink>

        <nav className={styles.nav}>
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                isActive ? `${styles.navLink} ${styles.navLinkActive}` : styles.navLink
              }
            >
              <span className={styles.navDot} aria-hidden />
              {item.label}
            </NavLink>
          ))}
        </nav>

        <div className={styles.sidebarFooter}>
          <span className={styles.sidebarFooterLabel}>workspace</span>
          <span className={styles.sidebarFooterValue}>colombia.open-graph</span>
        </div>
      </aside>

      <main className={styles.main}>
        <Outlet />
      </main>
    </div>
  );
}
