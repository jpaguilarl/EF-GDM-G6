import { useEffect, useRef, useState } from "react";

interface JobLogViewerProps {
  jobId: string | null;
  autoScroll?: boolean;
}

export function JobLogViewer({ jobId, autoScroll = true }: JobLogViewerProps) {
  const [lines, setLines] = useState<string[]>([]);
  const [done, setDone] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const tailRef = useRef<string[]>([]);

  useEffect(() => {
    if (!jobId) return;
    setLines([]);
    setDone(false);
    tailRef.current = [];

    const es = new EventSource(`/api/v1/panel/jobs/${jobId}/logs`);
    es.onmessage = (e) => {
      tailRef.current = [...tailRef.current.slice(-999), e.data];
      setLines([...tailRef.current]);
    };
    es.addEventListener("done", () => {
      setDone(true);
      es.close();
    });
    es.onerror = () => {
      setDone(true);
      es.close();
    };
    return () => es.close();
  }, [jobId]);

  useEffect(() => {
    if (autoScroll && containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  if (!jobId) return null;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <h3 className="text-label-md text-on-surface-variant">Logs</h3>
        {done && (
          <span className="text-caption text-on-surface-variant">Proceso finalizado</span>
        )}
      </div>
      <div
        ref={containerRef}
        className="bg-surface-dim text-on-surface rounded-DEFAULT p-4 font-mono text-caption overflow-auto max-h-96 border border-border-subtle"
      >
        {lines.length === 0 ? (
          <span className="text-on-surface-variant italic">
            Esperando logs...
          </span>
        ) : (
          lines.map((line, i) => (
            <div
              key={i}
              className={`${
                i % 2 === 0 ? "" : "bg-surface-muted/30"
              } px-1`}
            >
              {line}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
