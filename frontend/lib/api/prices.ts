import { apiFetch } from "./client";
import type {
  PricesStatusResponse,
  PricesUpdateRequest,
  PricesUpdateResponse,
} from "@/lib/types/prices";

export function getPricesStatus(): Promise<PricesStatusResponse> {
  return apiFetch<PricesStatusResponse>("/prices/status");
}

export function updatePrices(
  body: PricesUpdateRequest
): Promise<PricesUpdateResponse> {
  return apiFetch<PricesUpdateResponse>("/prices/update", {
    method: "POST",
    body: JSON.stringify(body),
  });
}
