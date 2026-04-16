import axios from "axios";

export const api = axios.create({ baseURL: "http://localhost:8000/api" });

export const fetchSummary = () => api.get("/summary").then(r => r.data);
export const fetchItems   = () => api.get("/prices/items").then(r => r.data);
export const fetchStores  = () => api.get("/prices/stores").then(r => r.data);

export const fetchPriceHistory = (itemId: string, storeId: string, days: number) =>
  api.get("/prices/history", { params: { item_id: itemId, store_id: storeId, days } }).then(r => r.data);

export const fetchRecommendations = (rec?: string) =>
  api.get("/recommendations", { params: rec ? { recommendation: rec } : {} }).then(r => r.data);

export const fetchSignals = (days: number, signalType?: string) =>
  api.get("/signals", { params: { days, ...(signalType ? { signal_type: signalType } : {}) } }).then(r => r.data);
