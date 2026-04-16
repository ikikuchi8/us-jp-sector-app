interface EmptyStateProps {
  message: string;
  hint?: string;
}

export function EmptyState({ message, hint }: EmptyStateProps) {
  return (
    <div className="text-center py-12 text-gray-400">
      <p className="text-sm font-medium">{message}</p>
      {hint && <p className="text-xs mt-1">{hint}</p>}
    </div>
  );
}
