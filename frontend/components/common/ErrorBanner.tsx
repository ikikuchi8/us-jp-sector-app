interface ErrorBannerProps {
  message: string;
  onRetry?: () => void;
}

export function ErrorBanner({ message, onRetry }: ErrorBannerProps) {
  return (
    <div className="rounded-md bg-red-50 border border-red-200 px-4 py-3 flex items-start gap-3">
      <span className="text-red-500 mt-0.5">⚠</span>
      <div className="flex-1">
        <p className="text-sm text-red-700">{message}</p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="mt-1 text-xs text-red-600 underline hover:text-red-800"
          >
            再試行
          </button>
        )}
      </div>
    </div>
  );
}
