interface StatItemProps {
  label: string;
  value: string | number | null;
  unit?: string;
  valueClass?: string;
}

export function StatItem({ label, value, unit, valueClass }: StatItemProps) {
  const displayValue = value === null || value === undefined ? "—" : value;
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs text-gray-500">{label}</span>
      <span className={`text-lg font-semibold tabular-nums ${valueClass ?? "text-gray-800"}`}>
        {displayValue}
        {value !== null && value !== undefined && unit && (
          <span className="text-sm font-normal text-gray-500 ml-0.5">{unit}</span>
        )}
      </span>
    </div>
  );
}
