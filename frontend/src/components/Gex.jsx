// Gex.jsx
import React, { useEffect, useMemo, useRef, useCallback } from "react";
import * as echarts from "echarts";

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}

function aggregateByStrike(options, { maxBars = 60 } = {}) {
  if (!Array.isArray(options) || options.length === 0) {
    return { rows: [], spot: NaN, nearestStrike: null };
  }

  const buckets = new Map();
  let spot = NaN;
  let nearestStrike = null;
  let minDistance = Infinity;

  for (let i = 0; i < options.length; i++) {
    const row = options[i] || {};

    const strike = toNum(row.STRIKE);
    const gex = toNum(row.GEX);
    const typ = String(row.OPTIONTYPE || "C").toUpperCase();
    const S = toNum(row.UNDERLYINGSETTLEPRICE);

    let oi = row.PREVOPENPOSITION === "" ? 0 : Number(row.PREVOPENPOSITION);
    if (!Number.isFinite(oi)) oi = 0;

    if (Number.isFinite(S)) {
      spot = S;
      const distance = Math.abs(strike - S);
      if (distance < minDistance && Number.isFinite(strike)) {
        minDistance = distance;
        nearestStrike = strike;
      }
    }

    if (!Number.isFinite(strike) || !Number.isFinite(gex)) continue;

    let bucket = buckets.get(strike);
    if (!bucket) {
      bucket = { strike, call: 0, put: 0, oi: 0 };
      buckets.set(strike, bucket);
    }

    bucket.oi += oi;

    if (typ === "P") bucket.put += gex;
    else bucket.call += gex;
  }

  let rows = Array.from(buckets.values());
  if (rows.length === 0) return { rows: [], spot, nearestStrike: null };

  for (const r of rows) {
    r.total = r.call - r.put;
    r.net = r.call + r.put;
    r.call /= 1e6;
    r.put /= 1e6;
    r.total /= 1e6;
    r.net /= 1e6;
  }

  rows = rows.filter(
    (r) => Math.abs(r.net) > 1e-12 || Math.abs(r.total) > 1e-12
  );

  rows.sort((a, b) => a.strike - b.strike);

  if (Number.isFinite(spot) && rows.length > 0 && nearestStrike !== null) {
    let idx = rows.findIndex((r) => r.strike === nearestStrike);
    if (idx === -1) {
      idx = 0;
      let best = Math.abs(rows[0].strike - spot);
      for (let i = 1; i < rows.length; i++) {
        const d = Math.abs(rows[i].strike - spot);
        if (d < best) {
          best = d;
          idx = i;
        }
      }
    }

    const half = Math.floor(maxBars / 2);
    const start = Math.max(0, idx - half);
    rows = rows.slice(start, start + maxBars);
  } else {
    rows = rows.slice(-maxBars);
  }

  rows.reverse();

  return { rows, spot, nearestStrike };
}

const GEX = React.memo(({ data, height = 320, className = "" }) => {
  const elRef = useRef(null);
  const chartRef = useRef(null);
  const resizeObserverRef = useRef(null);

  const options = useMemo(
    () => (Array.isArray(data?.options) ? data.options : []),
    [data]
  );

  const { rows, spot, nearestStrike } = useMemo(
    () => aggregateByStrike(options),
    [options]
  );

  const tooltipFormatter = useCallback(
    (params) => {
      const idx =
        Array.isArray(params) && params.length ? params[0].dataIndex : 0;
      const r = rows[idx];
      if (!r) return "";

      return `
      <div style="font-size:11px">
        <b>Strike ${r.strike}</b><br/>
        Call: ${r.call.toFixed(2)}M<br/>
        Put: ${r.put.toFixed(2)}M<br/>
        <b>Net: ${r.net.toFixed(2)}M</b><br/>
        <span style="opacity:.8">Total: ${r.total.toFixed(2)}M</span>
      </div>
    `;
    },
    [rows]
  );

  useEffect(() => {
    const el = elRef.current;
    if (!el) return;

    if (chartRef.current) {
      chartRef.current.dispose();
      chartRef.current = null;
    }

    try {
      const instance = echarts.init(el, undefined, {
        renderer: "canvas",
        useDirtyRect: true,
      });
      chartRef.current = instance;

      resizeObserverRef.current = new ResizeObserver(() => {
        if (chartRef.current && !chartRef.current.isDisposed()) {
          try {
            chartRef.current.resize();
          } catch (error) {
            console.warn("Chart resize error:", error);
          }
        }
      });

      resizeObserverRef.current.observe(el);
    } catch (error) {
      console.error("ECharts init error:", error);
    }

    return () => {
      if (resizeObserverRef.current) {
        resizeObserverRef.current.disconnect();
        resizeObserverRef.current = null;
      }

      if (chartRef.current && !chartRef.current.isDisposed()) {
        try {
          chartRef.current.dispose();
        } catch (error) {
          console.warn("Chart dispose error:", error);
        }
        chartRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || chart.isDisposed()) return;

    if (!rows.length) {
      chart.clear();
      chart.setOption({
        animation: false,
        title: {
          text: "Нет данных",
          left: "center",
          top: "middle",
          textStyle: { fontSize: 11, color: "#64748b" },
        },
      });
      return;
    }

    const yCats = rows.map((r) => String(r.strike));
    const dataNet = rows.map((r) => ({
      value: r.net,
      itemStyle: { color: r.net > 0 ? "#22c55e" : "#ef4444" },
    }));
    const dataTotal = rows.map((r) => r.total);

    let spotStrikeIndex = -1;
    if (nearestStrike !== null) {
      spotStrikeIndex = rows.findIndex((r) => r.strike === nearestStrike);
    }

    const option = {
      animation: false,
      backgroundColor: "transparent",
      grid: { left: 4, right: 4, top: 4, bottom: 16, containLabel: true },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow", axis: "y" },
        confine: true,
        formatter: tooltipFormatter,
      },
      xAxis: {
        type: "value",
        position: "top",
        axisLine: { lineStyle: { color: "#475569" } },
        axisLabel: { color: "#64748b", fontSize: 10 },
        splitLine: { show: true, lineStyle: { color: "#1e293b" } },
      },
      yAxis: {
        type: "category",
        data: yCats,
        axisLine: { lineStyle: { color: "#475569" } },
        axisLabel: { color: "#64748b", fontSize: 10 },
        splitLine: { show: false },
        inverse: true,
      },
      series: [
        {
          name: "Net (C - P)",
          type: "bar",
          barWidth: "70%",
          data: dataNet,
          large: true,
          largeThreshold: 500,
          markLine:
            spotStrikeIndex >= 0
              ? {
                  symbol: "none",
                  label: {
                    formatter: () =>
                      Number.isFinite(spot) ? `Price ≈ ${spot}` : "Price",
                    position: "insideEndTop",
                    color: "#0ea5e9",
                    fontSize: 10,
                  },
                  lineStyle: {
                    type: "solid",
                    width: 1,
                    color: "#0ea5e9",
                    opacity: 0.9,
                  },
                  data: [{ yAxis: yCats[spotStrikeIndex] }],
                }
              : undefined,
        },
        {
          name: "Total (C + P)",
          type: "line",
          symbol: "none",
          lineStyle: { type: "dashed", width: 1, color: "#8b5cf6" },
          data: dataTotal,
        },
      ],
    };

    try {
      chart.setOption(option, true);
    } catch (error) {
      console.error("ECharts setOption error:", error);
    }
  }, [rows, spot, nearestStrike, tooltipFormatter]);

  return (
    <div
      className={`rounded-lg border border-slate-700 bg-slate-900/60 p-1 ${className}`}
    >
      <div ref={elRef} className="w-full" style={{ height }} />
    </div>
  );
});

GEX.displayName = "GEX";

export default GEX;
