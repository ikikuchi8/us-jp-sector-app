/**
 * API クライアント基盤。
 *
 * すべての API 呼び出しはこのモジュールの apiFetch() を経由する。
 * - ベース URL は NEXT_PUBLIC_API_BASE_URL から取得
 * - 4xx/5xx は ApiError としてスロー
 * - ネットワークエラーも ApiError としてスロー
 */

const BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string
  ) {
    super(`API Error ${status}: ${detail}`);
    this.name = "ApiError";
  }
}

const TIMEOUT_MS = 15_000;

export async function apiFetch<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${BASE_URL}${path}`;

  // body を持つメソッドのみ Content-Type を付与。GET には不要。
  const method = (options.method ?? "GET").toUpperCase();
  const hasBody = ["POST", "PUT", "PATCH"].includes(method);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);

  let response: Response;
  try {
    response = await fetch(url, {
      headers: {
        ...(hasBody ? { "Content-Type": "application/json" } : {}),
        ...options.headers,
      },
      signal: controller.signal,
      ...options,
    });
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      throw new ApiError(0, `タイムアウト (${TIMEOUT_MS / 1000}秒)`);
    }
    throw new ApiError(0, e instanceof Error ? e.message : "Network error");
  } finally {
    clearTimeout(timeoutId);
  }

  if (!response.ok) {
    let detail = `HTTP ${response.status}`;
    try {
      const body = await response.json();
      detail = body?.detail ?? detail;
    } catch {
      // JSON パース失敗時はデフォルトメッセージを使う
    }
    throw new ApiError(response.status, detail);
  }

  return response.json() as Promise<T>;
}
