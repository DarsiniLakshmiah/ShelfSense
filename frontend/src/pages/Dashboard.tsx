import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { fetchItems, fetchPriceHistory, fetchStores, fetchSummary } from "../api/client";
import PriceChart from "../components/PriceChart";
import StatCard from "../components/StatCard";

export default function Dashboard() {
  const [selectedItem, setSelectedItem]   = useState("");
  const [selectedStore, setSelectedStore] = useState("");
  const [days, setDays]                   = useState(90);

  const { data: summary }  = useQuery({ queryKey: ["summary"],  queryFn: fetchSummary });
  const { data: items = [] } = useQuery({ queryKey: ["items"],  queryFn: fetchItems });
  const { data: stores = [] } = useQuery({ queryKey: ["stores"], queryFn: fetchStores });

  const { data: history = [], isLoading: loadingHistory } = useQuery({
    queryKey: ["history", selectedItem, selectedStore, days],
    queryFn: () => fetchPriceHistory(selectedItem, selectedStore, days),
    enabled: !!selectedItem && !!selectedStore,
  });

  const latestPrice = history.length ? history[history.length - 1].regular_price : null;
  const delta7d     = history.length ? history[history.length - 1].delta_7d : null;

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <StatCard label="Items Tracked"  value={summary?.total_items  ?? "—"} />
        <StatCard label="Stores"         value={summary?.total_stores ?? "—"} />
        <StatCard label="Buy Now"        value={summary?.buy_now_count ?? "—"} color="bg-green-50" />
        <StatCard label="Wait"           value={summary?.wait_count    ?? "—"} color="bg-red-50" />
        <StatCard label="Neutral"        value={summary?.neutral_count ?? "—"} color="bg-yellow-50" />
        <StatCard label="Last Updated"   value={summary?.last_updated ? summary.last_updated.slice(0, 10) : "—"} />
      </div>

      {/* Price history panel */}
      <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6 space-y-4">
        <h2 className="text-lg font-semibold text-gray-800">Price History</h2>

        {/* Controls */}
        <div className="flex flex-wrap gap-3">
          <select
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-400"
            value={selectedItem}
            onChange={e => setSelectedItem(e.target.value)}
          >
            <option value="">Select item…</option>
            {items.map((it: any) => (
              <option key={it.item_id} value={it.item_id}>{it.item_name}</option>
            ))}
          </select>

          <select
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-400"
            value={selectedStore}
            onChange={e => setSelectedStore(e.target.value)}
          >
            <option value="">Select store…</option>
            {stores.map((s: any) => (
              <option key={s.store_id} value={s.store_id}>{s.store_id}</option>
            ))}
          </select>

          <select
            className="border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-400"
            value={days}
            onChange={e => setDays(Number(e.target.value))}
          >
            {[14, 30, 60, 90, 180].map(d => (
              <option key={d} value={d}>Last {d} days</option>
            ))}
          </select>
        </div>

        {/* Mini metrics */}
        {latestPrice !== null && (
          <div className="flex gap-6 text-sm">
            <div>
              <span className="text-gray-400">Current Price </span>
              <span className="font-semibold text-gray-800">${latestPrice.toFixed(2)}</span>
            </div>
            {delta7d !== null && (
              <div>
                <span className="text-gray-400">7-Day Δ </span>
                <span className={`font-semibold ${delta7d < 0 ? "text-green-600" : "text-red-600"}`}>
                  {delta7d > 0 ? "+" : ""}{delta7d.toFixed(2)}
                </span>
              </div>
            )}
            <div>
              <span className="text-gray-400">Sale days highlighted in </span>
              <span className="text-yellow-500 font-semibold">yellow</span>
            </div>
          </div>
        )}

        {loadingHistory ? (
          <div className="h-64 flex items-center justify-center text-gray-400">Loading…</div>
        ) : (
          <PriceChart data={history} />
        )}
      </div>
    </div>
  );
}
