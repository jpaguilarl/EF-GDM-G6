import { useQuery } from "@tanstack/react-query";
import { REALTIME_BASE } from "../lib/realtimeViews";
import type { FraudRow, ClusterRow } from "../lib/types";

async function fetchRealtime<T>(path: string, params?: Record<string, string>): Promise<T[]> {
  const url = new URL(`${REALTIME_BASE}${path}`, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v) url.searchParams.set(k, v);
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`GET ${path}: ${res.status}`);
  return res.json();
}

export function useFraudList(params?: {
  is_fraud?: boolean;
  service_id?: string;
  ratecode_id?: string;
  limit?: number;
}) {
  const qp: Record<string, string> = {};
  if (params?.is_fraud !== undefined) qp.is_fraud = String(params.is_fraud);
  if (params?.service_id) qp.service_id = params.service_id;
  if (params?.ratecode_id) qp.ratecode_id = params.ratecode_id;
  if (params?.limit) qp.limit = String(params.limit);

  return useQuery<FraudRow[]>({
    queryKey: ["fraud", qp],
    queryFn: () => fetchRealtime<FraudRow>("/fraud", qp),
    refetchInterval: 5000,
  });
}

export function useClusterList(params?: {
  service_id?: string;
  cluster_id?: string;
}) {
  const qp: Record<string, string> = {};
  if (params?.service_id) qp.service_id = params.service_id;
  if (params?.cluster_id) qp.cluster_id = params.cluster_id;

  return useQuery<ClusterRow[]>({
    queryKey: ["clusters", qp],
    queryFn: () => fetchRealtime<ClusterRow>("/clusters", qp),
    refetchInterval: 5000,
  });
}
