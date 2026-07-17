import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { QueryClientProvider } from "@tanstack/react-query";
import { queryClient } from "./lib/api";
import { Layout } from "./components/Layout";
import { Configuration } from "./pages/Configuration";
import { Realtime } from "./pages/Realtime";
import { BatchLoad } from "./pages/BatchLoad";
import { GoldResults } from "./pages/GoldResults";
import { Audit } from "./pages/Audit";

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter basename="/panel">
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<Navigate to="/configuration" replace />} />
            <Route path="configuration" element={<Configuration />} />
            <Route path="realtime" element={<Realtime />} />
            <Route path="batch" element={<BatchLoad />} />
            <Route path="gold" element={<GoldResults />} />
            <Route path="audit" element={<Audit />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
