import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";
import Dashboard from "./pages/Dashboard";
import Recommendations from "./pages/Recommendations";
import Signals from "./pages/Signals";

const qc = new QueryClient({ defaultOptions: { queries: { staleTime: 60_000, retry: 1 } } });

const NAV = [
  { id: "dashboard",       label: "📈 Dashboard" },
  { id: "recommendations", label: "🛍 Recommendations" },
  { id: "signals",         label: "📡 Signals" },
];

export default function App() {
  const [page, setPage] = useState("dashboard");

  return (
    <QueryClientProvider client={qc}>
      <div className="min-h-screen bg-gray-50">
        {/* Top nav */}
        <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex items-center gap-6 h-16">
            <span className="text-xl font-bold text-blue-700 tracking-tight">🛒 ShelfSense</span>
            <span className="text-xs text-gray-400 hidden sm:block">Grocery Price Intelligence</span>
            <nav className="ml-auto flex gap-1">
              {NAV.map(n => (
                <button
                  key={n.id}
                  onClick={() => setPage(n.id)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    page === n.id
                      ? "bg-blue-600 text-white"
                      : "text-gray-600 hover:bg-gray-100"
                  }`}
                >
                  {n.label}
                </button>
              ))}
            </nav>
          </div>
        </header>

        {/* Page content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {page === "dashboard"       && <Dashboard />}
          {page === "recommendations" && <Recommendations />}
          {page === "signals"         && <Signals />}
        </main>
      </div>
    </QueryClientProvider>
  );
}
