import React from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
} from "recharts";

console.log("VolatilitySmile render");

const TriangleUp = ({ fill, x, y }) => (
  <path d={`M${x},${y} l6,10 l-12,0 Z`} fill={fill} />
);
const TriangleDown = ({ fill, x, y }) => (
  <path d={`M${x},${y} l6,-10 l-12,0 Z`} fill={fill} />
);

const VolatilitySmile = ({ grouped, strikes, underlyingPrice, histVol }) => {
  // Для теста: если grouped пустой → подставим мок
  const smileData =
    strikes.length > 0
      ? strikes.map((strike) => ({
          strike: Number(strike),
          ivC: Number(grouped[strike]?.C?.IV) || null,
          ivP: Number(grouped[strike]?.P?.IV) || null,
          callAsk: Number(grouped[strike]?.C?.ASK) || null,
          callBid: Number(grouped[strike]?.C?.BID) || null,
          putAsk: Number(grouped[strike]?.P?.ASK) || null,
          putBid: Number(grouped[strike]?.P?.BID) || null,
        }))
      : [
          { strike: 90, ivC: 20, ivP: 25, callAsk: 5, callBid: 4, putAsk: 6, putBid: 5 },
          { strike: 100, ivC: 22, ivP: 27, callAsk: 6, callBid: 5, putAsk: 7, putBid: 6 },
          { strike: 110, ivC: 24, ivP: 30, callAsk: 7, callBid: 6, putAsk: 8, putBid: 7 },
        ];

  const spot = Number(underlyingPrice) || 100;
  const hv = Number(histVol) || 23;

  console.log("SmileData:", smileData, "Spot:", spot, "HV:", hv);

  return (
    <div className="w-full lg:w-1/3 bg-slate-900/60 p-4 rounded-2xl shadow-lg backdrop-blur-xl">
      <h3 className="text-lg font-bold mb-3 text-neon-blue">
        Улыбка волатильности
      </h3>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={smileData}>
          <XAxis
            type="number"
            dataKey="strike"
            domain={["auto", "auto"]}
            stroke="#888"
          />
          <YAxis stroke="#888" />
          <Tooltip />

          {/* IV линии */}
          <Line dataKey="ivC" stroke="#3b82f6" name="Call IV" dot={false} />
          <Line dataKey="ivP" stroke="#ec4899" name="Put IV" dot={false} />

          {/* Hist Vol */}
          <ReferenceLine
            y={hv}
            stroke="#22c55e"
            strokeDasharray="4 4"
            label={{ value: `Hist Vol ${hv}`, fill: "#22c55e", position: "insideTopRight" }}
          />

          {/* Spot */}
          <ReferenceLine
            x={spot}
            stroke="#facc15"
            strokeDasharray="4 4"
            label={{ value: `Spot ${spot}`, fill: "#facc15", position: "insideBottomRight" }}
          />

          {/* Scatter точки */}
          <Scatter
            name="Call Ask"
            data={smileData.map((d) => ({ x: d.strike, y: d.callAsk }))}
            shape={(props) => <TriangleUp {...props} fill="#22c55e" />}
          />
          <Scatter
            name="Call Bid"
            data={smileData.map((d) => ({ x: d.strike, y: d.callBid }))}
            shape={(props) => <TriangleDown {...props} fill="#22c55e" />}
          />
          <Scatter
            name="Put Ask"
            data={smileData.map((d) => ({ x: d.strike, y: d.putAsk }))}
            shape={(props) => <TriangleUp {...props} fill="#ef4444" />}
          />
          <Scatter
            name="Put Bid"
            data={smileData.map((d) => ({ x: d.strike, y: d.putBid }))}
            shape={(props) => <TriangleDown {...props} fill="#ef4444" />}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
};

export default VolatilitySmile;