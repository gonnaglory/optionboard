// CandlesChart.jsx
import React, {
  useEffect,
  useRef,
  useState,
  useMemo,
  useCallback,
} from "react";
import * as echarts from "echarts";

const API_URL = import.meta.env.VITE_API_URL ?? "/api";

function useCandles(symbol) {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!symbol) {
      setData([]);
      setLoading(false);
      return;
    }

    let aborted = false;
    const ac = new AbortController();
    setLoading(true);
    setError(null);

    fetch(`${API_URL}/candles/${symbol}`, {
      signal: ac.signal,
      headers: { accept: "application/json" },
    })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}: ${r.statusText}`);
        return r.json();
      })
      .then((json) => {
        if (aborted) return;

        const arr = Array.isArray(json) ? json : [];
        const mapped = arr
          .map((d) => ({
            time: new Date(d.timestamp).getTime(),
            open: +d.open,
            high: +d.high,
            low: +d.low,
            close: +d.close,
            volume: d.volume != null ? +d.volume : 0,
          }))
          .sort((a, b) => a.time - b.time);

        setData(mapped);
      })
      .catch((e) => {
        if (aborted || e.name === "AbortError") return;
        console.error("Candles load error", e);
        setError(e.message || "Ошибка загрузки данных");
        setData([]);
      })
      .finally(() => {
        if (!aborted) setLoading(false);
      });

    return () => {
      aborted = true;
      ac.abort();
    };
  }, [symbol]);

  return { data, loading, error };
}

function useEChart(containerRef, options) {
  const chartRef = useRef(null);
  const resizeObserverRef = useRef(null);

  useEffect(() => {
    const el = containerRef.current;
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
  }, [containerRef]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !chartRef.current || chartRef.current.isDisposed()) return;

    try {
      chart.setOption(options, true);
    } catch (error) {
      console.error("ECharts setOption error:", error);
    }
  }, [options]);

  return chartRef;
}

export default function CandlesChart({ asset, height = 320, className = "" }) {
  const containerRef = useRef(null);
  const { data, loading, error } = useCandles(asset);

  const chartData = useMemo(() => {
    if (!data.length) return { categories: [], klineData: [] };

    const categories = [];
    const klineData = [];

    for (let i = 0; i < data.length; i++) {
      const d = data[i];
      const timestamp = d.time;

      const timeLabel = new Date(timestamp).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });

      categories.push(timeLabel);
      klineData.push([d.open, d.close, d.low, d.high]);
    }

    return { categories, klineData };
  }, [data]);

  const tooltipFormatter = useCallback(
    (params) => {
      const p = Array.isArray(params)
        ? params.find((x) => x.seriesType === "candlestick")
        : params;
      if (!p) return "";

      const idx = p.dataIndex;
      const d = data[idx];
      if (!d) return "";

      const dt = new Date(d.time).toLocaleString();

      return (
        `<div style="font-size:12px;line-height:1.4">` +
        `<div><b>${dt}</b></div>` +
        `<div>O: ${d.open}  H: ${d.high}  L: ${d.low}  C: ${d.close}</div>` +
        `</div>`
      );
    },
    [data]
  );

  const chartOptions = useMemo(() => {
    if (!chartData.categories.length) {
      return {
        title: {
          text: "Нет данных",
          left: "center",
          top: "middle",
          textStyle: { color: "#94a3b8", fontSize: 12 },
        },
      };
    }

    return {
      animation: false,
      backgroundColor: "transparent",
      useUTC: false,
      grid: {
        left: 32,
        right: 8,
        top: 4,
        bottom: 16,
        containLabel: true,
      },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        backgroundColor: "rgba(15, 23, 42, 0.95)",
        borderWidth: 0,
        textStyle: { color: "#fff" },
        confine: true,
        formatter: tooltipFormatter,
      },
      xAxis: {
        type: "category",
        data: chartData.categories,
        boundaryGap: true,
        axisLine: { lineStyle: { color: "#475569" } },
        axisLabel: { color: "#64748b", fontSize: 10 },
        splitLine: { show: true, lineStyle: { color: "#1e293b" } },
      },
      yAxis: {
        scale: true,
        position: "right",
        axisLabel: {
          color: "#64748b",
          fontSize: 10,
          formatter: (value) => value,
        },
        splitLine: { show: true, lineStyle: { color: "#1e293b" } },
      },
      dataZoom: [
        {
          type: "inside",
          filterMode: "filter",
          zoomOnMouseWheel: true,
          moveOnMouseWheel: true,
        },
      ],
      series: [
        {
          name: "Price",
          type: "candlestick",
          itemStyle: {
            color: "#10b981",
            color0: "#ef4444",
            borderColor: "#10b981",
            borderColor0: "#ef4444",
            borderWidth: 1,
          },
          large: true,
          largeThreshold: 1000,
          progressive: 500,
          progressiveThreshold: 2000,
          data: chartData.klineData,
        },
      ],
    };
  }, [chartData, tooltipFormatter]);

  const chartRef = useEChart(containerRef, chartOptions);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !chartData.categories.length || chart.isDisposed()) return;

    setTimeout(() => {
      if (chart && !chart.isDisposed()) {
        chart.dispatchAction({
          type: "dataZoom",
          start: 70,
          end: 100,
          animation: false,
        });
      }
    }, 100);
  }, [chartData.categories.length, chartRef]);

  return (
    <div
      className={`rounded-lg border border-slate-700 bg-slate-900/60 p-1 ${className}`}
    >
      <div ref={containerRef} className="w-full" style={{ height }} />

      {loading && (
        <div className="px-2 py-1 text-xs text-slate-400 text-center">
          Загрузка графика…
        </div>
      )}

      {error && !loading && (
        <div className="px-2 py-1 text-xs text-red-400 text-center">
          Ошибка: {error}
        </div>
      )}

      {!loading && !error && !data.length && (
        <div className="px-2 py-1 text-xs text-slate-400 text-center">
          Нет данных
        </div>
      )}
    </div>
  );
}
