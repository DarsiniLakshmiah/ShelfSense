interface Props { value: string }

const map: Record<string, string> = {
  BUY_NOW: "bg-green-100 text-green-800 border-green-200",
  WAIT:    "bg-red-100 text-red-800 border-red-200",
  NEUTRAL: "bg-yellow-100 text-yellow-800 border-yellow-200",
};

const icons: Record<string, string> = { BUY_NOW: "🟢", WAIT: "🔴", NEUTRAL: "🟡" };

export default function RecommendationBadge({ value }: Props) {
  const cls = map[value] ?? "bg-gray-100 text-gray-700";
  return (
    <span className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold border ${cls}`}>
      {icons[value]} {value.replace("_", " ")}
    </span>
  );
}
