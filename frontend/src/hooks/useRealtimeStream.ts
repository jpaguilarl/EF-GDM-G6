import { useEffect, useRef, useState } from "react";
import { REALTIME_BASE } from "../lib/realtimeViews";
import type { RealtimeViewConfig } from "../lib/types";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const parseJSON = (text: string): any => {
  try { return JSON.parse(text); } catch { return null; }
};

const BACKOFFS = [1000, 2000, 5000];

interface UseRealtimeStreamResult {
  rows: Record<string, unknown>[];
  status: "connecting" | "connected" | "reconnecting" | "closed";
  lastEventAt: number | null;
  lastError: string | null;
  reload: () => void;
}

export function useRealtimeStream(
  view: string,
  config: RealtimeViewConfig,
  enabled = true,
): UseRealtimeStreamResult {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [status, setStatus] = useState<"connecting" | "connected" | "reconnecting" | "closed">("connecting");
  const [lastEventAt, setLastEventAt] = useState<number | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);
  const [retryToken, setRetryToken] = useState(0);
  const attemptRef = useRef(0);
  const dedupKey = config.dedupKey;

  useEffect(() => {
    if (!enabled) {
      setRows([]);
      setStatus("closed");
      return;
    }

    const url = `${REALTIME_BASE}/${view}/stream`;

    setStatus(attemptRef.current === 0 ? "connecting" : "reconnecting");
    setRows([]);
    setLastError(null);

    let es: EventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    const connect = () => {
      es = new EventSource(url);

      es.addEventListener("snapshot", (e: MessageEvent) => {
        const data = parseJSON(e.data);
        if (!data) return;
        attemptRef.current = 0;
        setRows(data as Record<string, unknown>[]);
        setStatus("connected");
        setLastEventAt(Date.now());
      });

      es.addEventListener("increment", (e: MessageEvent) => {
        const row = parseJSON(e.data);
        if (!row || !dedupKey || dedupKey.length === 0) return;
        attemptRef.current = 0;
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
        es?.close();
        attemptRef.current += 1;
        if (attemptRef.current > 3) {
          setStatus("closed");
          setLastError("No se pudo conectar tras 3 intentos");
          return;
        }
        const delay = BACKOFFS[attemptRef.current - 1] ?? BACKOFFS[2];
        setStatus("reconnecting");
        reconnectTimer = setTimeout(connect, delay);
      };
    };

    connect();

    return () => {
      es?.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [view, enabled, retryToken, dedupKey.join(",")]);

  const reload = () => {
    attemptRef.current = 0;
    setStatus("connecting");
    setLastError(null);
    setRetryToken((t) => t + 1);
  };

  return { rows, status, lastEventAt, lastError, reload };
}
