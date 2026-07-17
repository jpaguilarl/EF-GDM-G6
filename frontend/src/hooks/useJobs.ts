import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiGet, apiPost } from "../lib/api";
import type { JobSummary, JobDetail } from "../lib/types";

export function useJobs() {
  return useQuery<JobSummary[]>({
    queryKey: ["jobs"],
    queryFn: () => apiGet<JobSummary[]>("/jobs"),
    refetchInterval: 3000,
  });
}

export function useJob(id: string | null) {
  return useQuery<JobDetail>({
    queryKey: ["job", id],
    queryFn: () => apiGet<JobDetail>(`/jobs/${id}`),
    enabled: !!id,
    refetchInterval: 3000,
  });
}

export function useSubmitBronzeJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { categories: string[]; year: number; month_start: number; month_end: number }) =>
      apiPost("/jobs/bronze", body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useSubmitSilverJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: {
      stage: string;
      categories?: string[];
      year?: number;
      month_start?: number;
      month_end?: number;
    }) => apiPost("/jobs/silver", body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useSubmitGoldJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: {
      mode?: string;
      only?: string[];
      categories?: string[];
      year?: number;
      month_start?: number;
      month_end?: number;
    }) => apiPost("/jobs/gold", body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useSubmitGoldMlJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { model: string }) => apiPost("/jobs/gold-ml", body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useStopJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => apiPost(`/jobs/${id}/stop`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}
