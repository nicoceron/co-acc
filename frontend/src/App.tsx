import { lazy, Suspense, useEffect } from "react";
import { Navigate, Route, Routes, useLocation, useParams } from "react-router";

import { AppShell } from "./components/common/AppShell";
import { PublicShell } from "./components/common/PublicShell";
import { Spinner } from "./components/common/Spinner";
import { IS_PATTERNS_ENABLED, IS_PUBLIC_MODE } from "./config/runtime";
import { Landing } from "./pages/Landing";
import { useAuthStore } from "./stores/auth";

const EntityAnalysis = lazy(() => import("./pages/EntityAnalysis").then((m) => ({ default: m.EntityAnalysis })));
const Baseline = lazy(() => import("./pages/Baseline").then((m) => ({ default: m.Baseline })));
const Dashboard = lazy(() => import("./pages/Dashboard").then((m) => ({ default: m.Dashboard })));
const Cases = lazy(() => import("./pages/Cases").then((m) => ({ default: m.Cases })));
const Investigations = lazy(() => import("./pages/Investigations").then((m) => ({ default: m.Investigations })));
const InvestigationDossier = lazy(() => import("./pages/InvestigationDossier").then((m) => ({ default: m.InvestigationDossier })));
const Login = lazy(() => import("./pages/Login").then((m) => ({ default: m.Login })));
const Patterns = lazy(() => import("./pages/Patterns").then((m) => ({ default: m.Patterns })));
const Register = lazy(() => import("./pages/Register").then((m) => ({ default: m.Register })));
const Results = lazy(() => import("./pages/Results").then((m) => ({ default: m.Results })));
const Search = lazy(() => import("./pages/Search").then((m) => ({ default: m.Search })));
const Signals = lazy(() => import("./pages/Signals").then((m) => ({ default: m.Signals })));
const SharedInvestigation = lazy(() => import("./pages/SharedInvestigation").then((m) => ({ default: m.SharedInvestigation })));

function LazyPage({ children }: { children: React.ReactNode }) {
  return <Suspense fallback={<Spinner />}>{children}</Suspense>;
}

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  const restored = useAuthStore((s) => s.restored);
  if (!restored) return <Spinner />;
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function RedirectIfAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  const restored = useAuthStore((s) => s.restored);
  if (!restored) return <Spinner />;
  if (token) return <Navigate to="/app" replace />;
  return <>{children}</>;
}

function GraphRedirect() {
  const { entityId } = useParams();
  return <Navigate to={`/app/analysis/${entityId}`} replace />;
}

export function App() {
  const restore = useAuthStore((s) => s.restore);
  const location = useLocation();

  useEffect(() => {
    if (
      location.pathname.startsWith("/results")
      || location.pathname.startsWith("/casos")
      || location.pathname.startsWith("/investigations")
      || location.pathname.startsWith("/biblioteca")
    ) {
      useAuthStore.setState((state) => ({ ...state, restored: true }));
      return;
    }
    restore();
  }, [location.pathname, restore]);

  return (
    <Routes>
      {/* Public shell — landing, login, register */}
      <Route
        element={IS_PUBLIC_MODE ? <PublicShell /> : (
          <RedirectIfAuth>
            <PublicShell />
          </RedirectIfAuth>
        )}
      >
        <Route index element={<Landing />} />
        <Route path="casos" element={<LazyPage><Results /></LazyPage>} />
        <Route path="casos/modalidad/:categoryId" element={<LazyPage><Results /></LazyPage>} />
        <Route path="biblioteca" element={<LazyPage><Results /></LazyPage>} />
        <Route path="biblioteca/modalidad/:categoryId" element={<LazyPage><Results /></LazyPage>} />
        <Route path="results" element={<LazyPage><Results /></LazyPage>} />
        <Route path="results/modalidad/:categoryId" element={<LazyPage><Results /></LazyPage>} />
        <Route path="investigations" element={<LazyPage><Results /></LazyPage>} />
        <Route path="investigations/modalidad/:categoryId" element={<LazyPage><Results /></LazyPage>} />
        <Route path="casos/:slug" element={<LazyPage><InvestigationDossier /></LazyPage>} />
        <Route path="biblioteca/:slug" element={<LazyPage><InvestigationDossier /></LazyPage>} />
        <Route path="investigations/:slug" element={<LazyPage><InvestigationDossier /></LazyPage>} />
        {!IS_PUBLIC_MODE && <Route path="login" element={<LazyPage><Login /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="register" element={<LazyPage><Register /></LazyPage>} />}
      </Route>

      {/* Public — shared investigation (no auth, no shell) */}
      <Route path="shared/:token" element={<LazyPage><SharedInvestigation /></LazyPage>} />

      {/* Authenticated shell — the intelligence workspace */}
      <Route
        path="app"
        element={IS_PUBLIC_MODE ? <AppShell /> : (
          <RequireAuth>
            <AppShell />
          </RequireAuth>
        )}
      >
        <Route index element={<LazyPage><Dashboard /></LazyPage>} />
        <Route path="search" element={<LazyPage><Search /></LazyPage>} />
        <Route path="analysis/:entityId" element={<LazyPage><EntityAnalysis /></LazyPage>} />
        <Route path="graph/:entityId" element={<GraphRedirect />} />
        {IS_PATTERNS_ENABLED && <Route path="patterns" element={<LazyPage><Patterns /></LazyPage>} />}
        {IS_PATTERNS_ENABLED && <Route path="patterns/:entityId" element={<LazyPage><Patterns /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="signals" element={<LazyPage><Signals /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="signals/:signalId" element={<LazyPage><Signals /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="cases" element={<LazyPage><Cases /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="cases/:caseId" element={<LazyPage><Cases /></LazyPage>} />}
        <Route path="baseline/:entityId" element={<LazyPage><Baseline /></LazyPage>} />
        {!IS_PUBLIC_MODE && <Route path="investigations" element={<LazyPage><Investigations /></LazyPage>} />}
        {!IS_PUBLIC_MODE && <Route path="investigations/:investigationId" element={<LazyPage><Investigations /></LazyPage>} />}
      </Route>

      {/* Catch-all */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
