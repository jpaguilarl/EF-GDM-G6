import { useEffect, useState } from "react";
import { REALTIME_BASE } from "../lib/realtimeViews";
import type { RealtimeViewConfig } from "../lib/types";

interface UseRealtimeStreamResult {
  rows: Record<string, unknown>[];
  status: "connecting" | "connected" | "reconnecting" | "closed";
  lastEventAt: number | null;
}

export function useRealtimeStream(
  view: string,
  config: RealtimeViewConfig,
  filters?: Record<string, string>,
  enabled = true,
): UseRealtimeStreamResult {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [status, setStatus] = useState<"connecting" | "connected" | "reconnecting" | "closed">("connecting");
  const [lastEventAt, setLastEventAt] = useState<number | null>(null);
  const dedupKey = config.dedupKey;

  const filtersKey = JSON.stringify(filters ?? {});

  useEffect(() => {
    if (!enabled) {
      setRows([]);
      setStatus("closed");
      return;
    }

    const params = new URLSearchParams();
    if (filters) {
      for (const [k, v] of Object.entries(filters)) {
        if (v) params.append(k, v);
      }
    }
    const qs = params.toString();
    const url = `${REALTIME_BASE}/${view}/stream${qs ? `?${qs}` : ""}`;

    setStatus("connecting");
    setRows([]);

    const es = new EventSource(url);

    es.addEventListener("snapshot", (e: MessageEvent) => {
      const data = JSON.parse(e.data) as Record<string, unknown>[];
      setRows(data);
      setStatus("connected");
      setLastEventAt(Date.now());
    });

    es.addEventListener("increment", (e: MessageEvent) => {
      const row = JSON.parse(e.data) as Record<string, unknown>;
      if (!dedupKey || dedupKey.length === 0) return;
      setRows((prev) => {
        const idx = prev.findIndex((r) =>
          dedupKey.every((k) => r[k] === row[k]),
        );
        if (idx >= 0) {
          const next = [...prev];
          next[idx] = row;
          return next;
        }
        return [...prev, row];
      });
      setLastEventAt(Date.now());
    });

    es.addEventListener("heartbeat", () => {
      setLastEventAt(Date.now());
    });

    es.onerror = () => {
      setStatus("reconnecting");
    };

    return () => es.close();
  }, [view, filtersKey, enabled, dedupKey.join(",")]);

  return { rows, status, lastEventAt };
}
