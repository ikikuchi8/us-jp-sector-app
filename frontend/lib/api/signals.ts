import { apiFetch } from "./client";
import type {
  SignalsGenerateRequest,
  SignalsGenerateResponse,
  SignalsLatestResponse,
} from "@/lib/types/signals";

export function getLatestSignals(
  signal_type = "simple_v1"
): Promise<SignalsLatestResponse> {
  return apiFetch<SignalsLatestResponse>(
    `/signals/latest?signal_type=${encodeURIComponent(signal_type)}`
  );
}

export function generateSignals(
  body: SignalsGenerateRequest
): Promise<SignalsGenerateResponse> {
  return apiFetch<SignalsGenerateResponse>("/signals/generate", {
    method: "POST",
    body: JSON.stringify(body),
  });
}
