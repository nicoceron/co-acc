import { Navigate, Route, Routes } from "react-router";

import { AtlasShell } from "./components/Shell";
import { CaseDetail, Cases } from "./pages/Cases";
import { Dashboard } from "./pages/Dashboard";
import { EntityPage } from "./pages/Entity";
import { Landing } from "./pages/Landing";
import { PatternsPage } from "./pages/Patterns";
import { SearchPage } from "./pages/Search";
import { Sectors } from "./pages/Sectors";
import { SignalsPage } from "./pages/Signals";

export function AtlasApp() {
  return (
    <Routes>
      <Route element={<AtlasShell />}>
        <Route index element={<Landing />} />
        <Route path="casos" element={<Cases />} />
        <Route path="casos/:slug" element={<CaseDetail />} />
        <Route path="sectores" element={<Sectors />} />
        <Route path="app" element={<Dashboard />} />
        <Route path="app/search" element={<SearchPage />} />
        <Route path="app/entity" element={<EntityPage />} />
        <Route path="app/entity/:entityId" element={<EntityPage />} />
        <Route path="app/patterns" element={<PatternsPage />} />
        <Route path="app/signals" element={<SignalsPage />} />
        <Route path="app/signals/:signalId" element={<SignalsPage />} />

        <Route path="results/*" element={<Navigate to="/casos" replace />} />
        <Route path="biblioteca/*" element={<Navigate to="/casos" replace />} />
        <Route path="investigations/*" element={<Navigate to="/casos" replace />} />
        <Route path="login" element={<Navigate to="/app" replace />} />
        <Route path="register" element={<Navigate to="/app" replace />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
