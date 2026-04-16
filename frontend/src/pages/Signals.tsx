import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchSignals } from "../api/client";

const SIGNAL_TYPES = ["USDA", "BLS", "NOAA"];

export default function Signals() {
  const [days, setDays]       = useState(90);
  const [sigType, setSigType] = useState("USDA");

  const { data: rows = [], isLoading } = useQuery({
    queryKey: ["signals", days, sigType],
    queryFn: () => fetchSignals(days, sigType),
  });

  // Group by commodity for multi-line chart
  const commodities = [...new Set(rows.map((r: any) => r.commodity).filter(Boolean))];
  const byDate: Record<string, any> = {};
  rows.forEach((r: any) => {
    if (!byDate[r.date]) byDate[r.date] = { date: r.date };
    if (r.commodity) byDate[r.date][r.commodity] = r.value;
  });
  const chartData = Object.values(byDate).sort((a: any, b: any) => a.date.localeCompare(b.date));

  const COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"];

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
        <h2 className="text-lg font-semibold text-gray-800 mb-4">External Signals</h2>

        <div className="flex flex-wrap gap-3 mb-6">
          <div className="flex gap-2">
            {SIGNAL_TYPES.map(t => (
              <button
                key={t}
                onClick={() => setSigType(t)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium border transition-colors ${
                  sigType === t
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-gray-600 border-gray-200 hover:border-blue-400"
                }`}
              >
                {t}
              </button>
            ))}
          </div>
          <select
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-400"
            value={days}
            onChange={e => setDays(Number(e.target.value))}
          >
            {[30, 60, 90, 180, 365].map(d => (
              <option key={d} value={d}>Last {d} days</option>
            ))}
          </select>
        </div>

        {isLoading ? (
          <div className="h-64 flex items-center justify-center text-gray-400">Loading…</div>
        ) : chartData.length === 0 ? (
          <div className="h-64 flex items-center justify-center text-gray-400">
            No {sigType} signals yet — run the weekly signals DAG first
          </div>
        ) : (
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{ top: 8, right: 20, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="date" tick={{ fontSize: 11 }} tickFormatter={d => d.slice(5)} interval="preserveStartEnd" />
                <YAxis tick={{ fontSize: 11 }} width={60} />
                <Tooltip labelFormatter={l => `Date: ${l}`} />
                <Legend />
                {commodities.slice(0, 6).map((c, i) => (
                  <Line
                    key={c as string}
                    type="monotone"
                    dataKey={c as string}
                    stroke={COLORS[i % COLORS.length]}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Raw table */}
        {rows.length > 0 && (
          <div className="mt-6 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 text-left text-xs text-gray-400 uppercase tracking-wider">
                  <th className="pb-2 pr-4">Date</th>
                  <th className="pb-2 pr-4">Type</th>
                  <th className="pb-2 pr-4">Commodity</th>
                  <th className="pb-2 pr-4">Value</th>
                  <th className="pb-2">YoY Δ%</th>
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, 50).map((r: any, i: number) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 pr-4 text-gray-600">{r.date}</td>
                    <td className="py-2 pr-4 font-medium">{r.signal_type}</td>
                    <td className="py-2 pr-4 text-gray-600">{r.commodity}</td>
                    <td className="py-2 pr-4 font-semibold">{r.value?.toFixed(2) ?? "—"}</td>
                    <td className={`py-2 font-medium ${r.yoy_change_pct > 0 ? "text-red-500" : "text-green-500"}`}>
                      {r.yoy_change_pct ? `${r.yoy_change_pct > 0 ? "+" : ""}${r.yoy_change_pct.toFixed(1)}%` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
