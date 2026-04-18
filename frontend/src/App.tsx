import { lazy, Suspense } from "react";
import { Navigate, Route, Routes, useParams } from "react-router";

import { AppShell } from "@/components/common/AppShell";
import { PublicShell } from "@/components/common/PublicShell";
import { Spinner } from "@/components/common/Spinner";
import { IS_PATTERNS_ENABLED, IS_PUBLIC_MODE } from "@/config/runtime";

import { Landing } from "@/pages/Landing";

const Casos = lazy(() => import("@/pages/Casos").then((m) => ({ default: m.Casos })));
const CasoDetail = lazy(() =>
  import("@/pages/CasoDetail").then((m) => ({ default: m.CasoDetail })),
);
const Sector = lazy(() => import("@/pages/Sector").then((m) => ({ default: m.Sector })));
const Login = lazy(() => import("@/pages/Login").then((m) => ({ default: m.Login })));
const Register = lazy(() => import("@/pages/Register").then((m) => ({ default: m.Register })));
const SharedInvestigation = lazy(() =>
  import("@/pages/SharedInvestigation").then((m) => ({ default: m.SharedInvestigation })),
);

const Dashboard = lazy(() =>
  import("@/pages/Dashboard").then((m) => ({ default: m.Dashboard })),
);
const Search = lazy(() => import("@/pages/Search").then((m) => ({ default: m.Search })));
const EntityAnalysis = lazy(() =>
  import("@/pages/EntityAnalysis").then((m) => ({ default: m.EntityAnalysis })),
);
const Patterns = lazy(() => import("@/pages/Patterns").then((m) => ({ default: m.Patterns })));
const Signals = lazy(() => import("@/pages/Signals").then((m) => ({ default: m.Signals })));
const Cases = lazy(() => import("@/pages/Cases").then((m) => ({ default: m.Cases })));

function GraphRedirect() {
  const { entityId } = useParams();
  return <Navigate to={`/app/analysis/${entityId}`} replace />;
}

function LazyRoute({ children }: { children: React.ReactNode }) {
  return <Suspense fallback={<Spinner />}>{children}</Suspense>;
}

export function App() {
  return (
    <Routes>
      <Route element={<PublicShell />}>
        <Route index element={<Landing />} />
        <Route path="casos" element={<LazyRoute><Casos /></LazyRoute>} />
        <Route path="casos/:slug" element={<LazyRoute><CasoDetail /></LazyRoute>} />
        <Route path="sector/:sectorId" element={<LazyRoute><Sector /></LazyRoute>} />
        {!IS_PUBLIC_MODE && <Route path="login" element={<LazyRoute><Login /></LazyRoute>} />}
        {!IS_PUBLIC_MODE && (
          <Route path="register" element={<LazyRoute><Register /></LazyRoute>} />
        )}
      </Route>

      <Route
        path="shared/:token"
        element={<LazyRoute><SharedInvestigation /></LazyRoute>}
      />

      <Route path="app" element={<AppShell />}>
        <Route index element={<LazyRoute><Dashboard /></LazyRoute>} />
        <Route path="search" element={<LazyRoute><Search /></LazyRoute>} />
        <Route
          path="analysis/:entityId"
          element={<LazyRoute><EntityAnalysis /></LazyRoute>}
        />
        <Route path="graph/:entityId" element={<GraphRedirect />} />
        {IS_PATTERNS_ENABLED && (
          <Route path="patterns" element={<LazyRoute><Patterns /></LazyRoute>} />
        )}
        {IS_PATTERNS_ENABLED && (
          <Route
            path="patterns/:entityId"
            element={<LazyRoute><Patterns /></LazyRoute>}
          />
        )}
        <Route path="signals" element={<LazyRoute><Signals /></LazyRoute>} />
        <Route path="signals/:signalId" element={<LazyRoute><Signals /></LazyRoute>} />
        <Route path="cases" element={<LazyRoute><Cases /></LazyRoute>} />
        <Route path="cases/:caseId" element={<LazyRoute><Cases /></LazyRoute>} />
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
