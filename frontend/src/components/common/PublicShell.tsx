import { NavLink, Outlet } from "react-router";

import styles from "./PublicShell.module.css";

export function PublicShell() {
  return (
    <div className={styles.shell}>
      <header className={styles.header}>
        <NavLink to="/" className={styles.brand}>
          <span className={styles.brandMark}>co/</span>
          <span className={styles.brandName}>acc</span>
        </NavLink>
        <nav className={styles.nav}>
          <NavLink
            to="/casos"
            className={({ isActive }) =>
              isActive ? `${styles.navLink} ${styles.navLinkActive}` : styles.navLink
            }
          >
            Casos
          </NavLink>
          <NavLink
            to="/sector/procurement"
            className={({ isActive }) =>
              isActive ? `${styles.navLink} ${styles.navLinkActive}` : styles.navLink
            }
          >
            Sectores
          </NavLink>
          <a
            href="https://github.com/nicoceron/co-acc"
            target="_blank"
            rel="noreferrer"
            className={styles.navLink}
          >
            GitHub
          </a>
        </nav>
      </header>

      <main className={styles.main}>
        <Outlet />
      </main>

      <footer className={styles.footer}>
        <div className={styles.footerInner}>
          <span className={styles.footerNote}>
            Contexto documental. No constituye acusación ni puntaje de riesgo.
          </span>
          <span className={styles.footerMeta}>
            <span className={styles.footerChip}>CO</span>
            <span>open graph infrastructure</span>
          </span>
        </div>
      </footer>
    </div>
  );
}
