import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

interface PricePoint {
  date: string;
  regular_price: number;
  promo_price: number | null;
  is_on_sale: boolean;
  delta_7d: number | null;
}

interface Props { data: PricePoint[] }

const fmt = (v: number) => `$${v.toFixed(2)}`;

export default function PriceChart({ data }: Props) {
  if (!data.length) return (
    <div className="flex items-center justify-center h-64 text-gray-400">No price data for this selection</div>
  );

  // Annotate sale days
  const saleDays = data.filter(d => d.is_on_sale).map(d => d.date);

  return (
    <div className="w-full h-72">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 20, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 11 }}
            tickFormatter={d => d.slice(5)}  // show MM-DD
            interval="preserveStartEnd"
          />
          <YAxis tickFormatter={fmt} tick={{ fontSize: 11 }} width={58} />
          <Tooltip formatter={(v: number) => fmt(v)} labelFormatter={l => `Date: ${l}`} />
          <Legend />
          {saleDays.map(d => (
            <ReferenceLine key={d} x={d} stroke="#fbbf24" strokeOpacity={0.4} />
          ))}
          <Line
            type="monotone"
            dataKey="regular_price"
            name="Regular Price"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 5 }}
          />
          <Line
            type="monotone"
            dataKey="promo_price"
            name="Promo Price"
            stroke="#ef4444"
            strokeWidth={2}
            dot={{ r: 4, fill: "#ef4444" }}
            connectNulls={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
