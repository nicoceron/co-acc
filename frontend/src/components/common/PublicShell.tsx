import { Link, Outlet } from "react-router";

import { IS_PUBLIC_MODE } from "@/config/runtime";
import { useAuthStore } from "@/stores/auth";

import styles from "./PublicShell.module.css";

export function PublicShell() {
  const token = useAuthStore((s) => s.token);

  return (
    <div className={styles.shell}>
      <header className={styles.header}>
        <div className={styles.brandBlock}>
          <Link to="/" className={styles.logo}>
            CO·ACC
          </Link>
          <span className={styles.brandNote}>hallazgos públicos con datos reales de Colombia</span>
        </div>

        <nav className={styles.nav}>
          <Link to="/" className={styles.navLink}>
            Inicio
          </Link>
          <Link to="/results" className={styles.navLink}>
            Descubrir
          </Link>
          <Link to="/investigations" className={styles.navLink}>
            Biblioteca
          </Link>
          <Link to="/#metodologia" className={styles.navLink}>
            Método
          </Link>
        </nav>

        <div className={styles.actions}>
          {IS_PUBLIC_MODE ? (
            <Link to="/app/search" className={styles.primaryLink}>
              Abrir grafo
            </Link>
          ) : !token && (
            <>
              <Link to="/login" className={styles.secondaryLink}>
                Ingresar
              </Link>
              <Link to="/register" className={styles.primaryLink}>
                Crear cuenta
              </Link>
            </>
          )}
        </div>
      </header>
      <main className={styles.content}>
        <Outlet />
      </main>
    </div>
  );
}
