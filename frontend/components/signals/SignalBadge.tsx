interface SignalBadgeProps {
  side: "long" | "short" | "neutral" | string;
}

const STYLES: Record<string, string> = {
  long:    "bg-green-100 text-green-800 border border-green-200",
  short:   "bg-red-100 text-red-800 border border-red-200",
  neutral: "bg-gray-100 text-gray-600 border border-gray-200",
};

const LABELS: Record<string, string> = {
  long:    "LONG",
  short:   "SHORT",
  neutral: "NEUTRAL",
};

export function SignalBadge({ side }: SignalBadgeProps) {
  const cls = STYLES[side] ?? STYLES.neutral;
  const label = LABELS[side] ?? side.toUpperCase();
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-semibold ${cls}`}>
      {label}
    </span>
  );
}
