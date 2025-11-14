// VolatilitySmile.jsx
import React, { memo, useMemo } from "react";
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

const TriangleUp = React.memo(({ fill, x, y }) => (
  <path d={`M${x},${y} l4,8 l-8,0 Z`} fill={fill} />
));
TriangleUp.displayName = "TriangleUp";

const TriangleDown = React.memo(({ fill, x, y }) => (
  <path d={`M${x},${y} l4,-8 l-8,0 Z`} fill={fill} />
));
TriangleDown.displayName = "TriangleDown";

const CustomTooltip = React.memo(({ active, payload, label }) => {
  if (!active || !payload) return null;

  return (
    <div className="bg-slate-900/95 border border-slate-700 rounded p-2 text-xs">
      <p className="text-slate-300 mb-1">Strike: {label}</p>
      {payload.map((entry, index) => (
        <p key={index} style={{ color: entry.color }}>
          {entry.name}: {entry.value}
        </p>
      ))}
    </div>
  );
});
CustomTooltip.displayName = "CustomTooltip";

const VolatilitySmile = memo(({ data = [], height = 480 }) => {
  // Преобразуем данные для VolatilitySmile
  const { grouped, strikes, underlyingPrice, histVol } = useMemo(() => {
    const grouped = {};
    const strikes = new Set();
    let underlyingPrice = null;
    let histVol = null;

    data.forEach((item) => {
      if (!item) return;

      const strike = item.STRIKE;
      if (strike) {
        strikes.add(strike);

        if (!grouped[strike]) {
          grouped[strike] = { C: null, P: null };
        }

        const type = item.OPTIONTYPE?.toUpperCase();
        if (type === "C" || type === "P") {
          grouped[strike][type] = {
            IV: item.IMPLIED_VOL,
            ASK: item.ASK,
            BID: item.BID,
          };
        }
      }

      // Берем первую попавшуюся цену и волатильность
      if (!underlyingPrice && item.UNDERLYINGSETTLEPRICE) {
        underlyingPrice = item.UNDERLYINGSETTLEPRICE;
      }
      if (!histVol && item.HIST_VOL) {
        histVol = item.HIST_VOL;
      }
    });

    return {
      grouped,
      strikes: Array.from(strikes).sort((a, b) => a - b),
      underlyingPrice,
      histVol,
    };
  }, [data]);

  const smileData = useMemo(() => {
    if (!strikes.length) return [];

    return strikes
      .map((strike) => {
        const strikeStr = String(strike);
        const callData = grouped[strikeStr]?.C;
        const putData = grouped[strikeStr]?.P;

        return {
          strike: Number(strike),
          ivC: callData?.IV ? Number(callData.IV) : null,
          ivP: putData?.IV ? Number(putData.IV) : null,
          callAsk: callData?.ASK ? Number(callData.ASK) : null,
          callBid: callData?.BID ? Number(callData.BID) : null,
          putAsk: putData?.ASK ? Number(putData.ASK) : null,
          putBid: putData?.BID ? Number(putData.BID) : null,
        };
      })
      .filter((item) => item.ivC !== null || item.ivP !== null);
  }, [grouped, strikes]);

  const hv = useMemo(
    () => (Number.isFinite(histVol) ? Number(histVol) : null),
    [histVol]
  );

  const spotPrice = useMemo(
    () => (Number.isFinite(underlyingPrice) ? Number(underlyingPrice) : null),
    [underlyingPrice]
  );

  const scatterData = useMemo(
    () => ({
      callBid: smileData
        .map((d) => ({ x: d.strike, y: d.callBid }))
        .filter((d) => d.y !== null),
      callAsk: smileData
        .map((d) => ({ x: d.strike, y: d.callAsk }))
        .filter((d) => d.y !== null),
      putBid: smileData
        .map((d) => ({ x: d.strike, y: d.putBid }))
        .filter((d) => d.y !== null),
      putAsk: smileData
        .map((d) => ({ x: d.strike, y: d.putAsk }))
        .filter((d) => d.y !== null),
    }),
    [smileData]
  );

  if (!smileData.length) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-900/60 p-2 h-full flex flex-col">
        <h3 className="font-semibold text-slate-100 text-sm mb-1">
          Volatility Smile
        </h3>
        <div className="flex-1 flex items-center justify-center text-slate-500 text-xs">
          Нет данных для отображения
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-900/60 p-2 h-full flex flex-col">
      <h3 className="font-semibold text-slate-100 text-sm mb-1">
        Volatility Smile
      </h3>
      <div className="flex-1 min-h-0">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={smileData}
            margin={{ left: 4, right: 4, top: 4, bottom: 4 }}
          >
            <XAxis
              type="number"
              dataKey="strike"
              domain={["auto", "auto"]}
              stroke="#64748b"
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickSize={4}
            />
            <YAxis
              stroke="#64748b"
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickSize={4}
            />
            <Tooltip content={<CustomTooltip />} />
            <Line
              dataKey="ivC"
              stroke="#3b82f6"
              name="Call IV"
              dot={false}
              strokeWidth={1.5}
            />
            <Line
              dataKey="ivP"
              stroke="#ec4899"
              name="Put IV"
              dot={false}
              strokeWidth={1.5}
            />
            {hv != null && (
              <ReferenceLine
                y={hv}
                stroke="#22d3ee"
                strokeDasharray="3 3"
                strokeWidth={1}
              />
            )}
            {spotPrice != null && (
              <ReferenceLine
                x={spotPrice}
                stroke="#f59e0b"
                strokeDasharray="2 2"
                strokeWidth={1}
              />
            )}
            <Scatter
              name="Call Bid"
              data={scatterData.callBid}
              shape={<TriangleUp fill="#10b981" />}
            />
            <Scatter
              name="Call Ask"
              data={scatterData.callAsk}
              shape={<TriangleDown fill="#10b981" />}
            />
            <Scatter
              name="Put Bid"
              data={scatterData.putBid}
              shape={<TriangleUp fill="#ef4444" />}
            />
            <Scatter
              name="Put Ask"
              data={scatterData.putAsk}
              shape={<TriangleDown fill="#ef4444" />}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="flex flex-wrap gap-2 mt-2 text-xs">
        <div className="flex items-center gap-1">
          <div className="w-3 h-0.5 bg-blue-500"></div>
          <span className="text-slate-400">Call IV</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-3 h-0.5 bg-pink-500"></div>
          <span className="text-slate-400">Put IV</span>
        </div>
        {hv != null && (
          <div className="flex items-center gap-1">
            <div className="w-3 h-0.5 bg-cyan-400 border-dashed border"></div>
            <span className="text-slate-400">Hist Vol</span>
          </div>
        )}
      </div>
    </div>
  );
});

VolatilitySmile.displayName = "VolatilitySmile";

export default VolatilitySmile;
