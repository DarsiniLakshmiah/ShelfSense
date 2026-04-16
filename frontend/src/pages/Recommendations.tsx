import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { fetchRecommendations } from "../api/client";
import RecommendationBadge from "../components/RecommendationBadge";

const REC_TYPES = ["", "BUY_NOW", "NEUTRAL", "WAIT"];

export default function Recommendations() {
  const [filter, setFilter] = useState("");
  const [search, setSearch] = useState("");

  const { data: rows = [], isLoading } = useQuery({
    queryKey: ["recommendations", filter],
    queryFn: () => fetchRecommendations(filter || undefined),
  });

  const visible = rows.filter((r: any) =>
    !search || r.item_id.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6">
        <h2 className="text-lg font-semibold text-gray-800 mb-4">Buy Recommendations</h2>

        {/* Filter bar */}
        <div className="flex flex-wrap gap-3 mb-4">
          <input
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm w-56 focus:outline-none focus:ring-2 focus:ring-blue-400"
            placeholder="Search item ID…"
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          <div className="flex gap-2">
            {REC_TYPES.map(t => (
              <button
                key={t}
                onClick={() => setFilter(t)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium border transition-colors ${
                  filter === t
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-gray-600 border-gray-200 hover:border-blue-400"
                }`}
              >
                {t || "All"}
              </button>
            ))}
          </div>
        </div>

        {isLoading ? (
          <div className="h-40 flex items-center justify-center text-gray-400">Loading…</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 text-left text-xs text-gray-400 uppercase tracking-wider">
                  <th className="pb-3 pr-4">Item ID</th>
                  <th className="pb-3 pr-4">Store</th>
                  <th className="pb-3 pr-4">Current Price</th>
                  <th className="pb-3 pr-4">Predicted 7d</th>
                  <th className="pb-3 pr-4">Predicted 14d</th>
                  <th className="pb-3 pr-4">Probability</th>
                  <th className="pb-3">Signal</th>
                </tr>
              </thead>
              <tbody>
                {visible.length === 0 && (
                  <tr><td colSpan={7} className="py-8 text-center text-gray-400">No signals found</td></tr>
                )}
                {visible.map((r: any, i: number) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
                    <td className="py-3 pr-4 font-mono text-xs text-gray-700">{r.item_id}</td>
                    <td className="py-3 pr-4 text-gray-600">{r.store_id}</td>
                    <td className="py-3 pr-4 font-semibold">${r.regular_price?.toFixed(2)}</td>
                    <td className="py-3 pr-4 text-blue-600">{r.predicted_7d ? `$${r.predicted_7d.toFixed(2)}` : "—"}</td>
                    <td className="py-3 pr-4 text-blue-600">{r.predicted_14d ? `$${r.predicted_14d.toFixed(2)}` : "—"}</td>
                    <td className="py-3 pr-4">
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-20 bg-gray-100 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-blue-500 rounded-full"
                            style={{ width: `${(r.probability * 100).toFixed(0)}%` }}
                          />
                        </div>
                        <span className="text-xs text-gray-500">{(r.probability * 100).toFixed(0)}%</span>
                      </div>
                    </td>
                    <td className="py-3"><RecommendationBadge value={r.recommendation} /></td>
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
