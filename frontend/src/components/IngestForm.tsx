import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Send, CheckCircle, XCircle } from "lucide-react";
import { tripTemplates } from "../lib/tripTemplates";

const BASE = "/api/v1";
const CATEGORIES = ["yellow", "green", "fhv", "fhvhv"] as const;

async function ingestTrip(body: unknown) {
  const res = await fetch(`${BASE}/ingest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}

interface IngestFormProps {
  className?: string;
}

export function IngestForm({ className }: IngestFormProps) {
  const [category, setCategory] = useState<string>("yellow");
  const [json, setJson] = useState(() => JSON.stringify(tripTemplates["yellow"], null, 2));
  const [result, setResult] = useState<Record<string, unknown> | null>(null);

  const mutation = useMutation({
    mutationFn: ingestTrip,
    onSuccess: (data) => setResult(data),
    onError: (err: Error) => setResult({ status: "error", message: err.message }),
  });

  const handleCategoryChange = (cat: string) => {
    setCategory(cat);
    setJson(JSON.stringify(tripTemplates[cat] || {}, null, 2));
    setResult(null);
  };

  const handleSubmit = () => {
    try {
      const body = JSON.parse(json);
      mutation.mutate(body);
    } catch {
      setResult({ status: "error", message: "JSON inválido" });
    }
  };

  const statusColor = (status: string) => {
    switch (status) {
      case "accepted": return "text-green-700 bg-green-50 border-green-200";
      case "rejected": return "text-orange-700 bg-orange-50 border-orange-200";
      case "error": return "text-error bg-error-container border-error/20";
      default: return "text-on-surface bg-surface-muted";
    }
  };

  return (
    <div className={`space-y-4 ${className ?? ""}`}>
      <div className="flex gap-1.5 flex-wrap">
        {CATEGORIES.map((cat) => (
          <button
            key={cat}
            onClick={() => handleCategoryChange(cat)}
            className={`px-3 py-1.5 rounded-DEFAULT text-caption border transition-colors ${
              category === cat
                ? "bg-primary-container text-on-primary-container border-primary-container"
                : "bg-surface-container-lowest text-on-surface border-border-subtle hover:bg-surface-muted"
            }`}
          >
            {cat.toUpperCase()}
          </button>
        ))}
      </div>

      <div>
        <label className="text-caption text-on-surface-variant block mb-1">Payload JSON</label>
        <textarea
          value={json}
          onChange={(e) => setJson(e.target.value)}
          rows={10}
          className="w-full bg-surface-muted border border-border-subtle rounded-DEFAULT p-3 font-mono text-caption text-on-surface focus:outline-none focus:ring-1 focus:ring-primary-container resize-y"
        />
      </div>

      <button
        onClick={handleSubmit}
        disabled={mutation.isPending}
        className="flex items-center gap-2 px-4 py-2 bg-primary-container text-on-primary-container rounded-DEFAULT text-label-md font-semibold hover:bg-primary-container/90 disabled:opacity-50 transition-colors"
      >
        <Send className="w-4 h-4" />
        {mutation.isPending ? "Enviando..." : "Enviar Viaje"}
      </button>

      {result && (
        <div className={`rounded-DEFAULT border p-3 space-y-1.5 ${statusColor(result.status as string)}`}>
          <div className="flex items-center gap-2">
            {result.status === "accepted" ? (
              <CheckCircle className="w-4 h-4" />
            ) : (
              <XCircle className="w-4 h-4" />
            )}
            <span className="text-label-md font-semibold capitalize">
              {result.status as string}
            </span>
          </div>
          <pre className="text-caption font-mono whitespace-pre-wrap">
            {JSON.stringify(result, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
