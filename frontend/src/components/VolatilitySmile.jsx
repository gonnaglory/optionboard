// VolatilitySmile.jsx
import React, { memo, useMemo, useRef, useEffect } from "react";
import * as echarts from "echarts";

const VolatilitySmile = memo(({ data = [], height = 480 }) => {
  const chartRef = useRef(null);
  const chartInstance = useRef(null);

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

  // Подготавливаем данные для ECharts
  const chartData = useMemo(() => {
    const callIVData = [];
    const putIVData = [];
    const callBidData = [];
    const callAskData = [];
    const putBidData = [];
    const putAskData = [];

    smileData.forEach((item) => {
      if (item.ivC !== null) {
        callIVData.push([item.strike, item.ivC]);
      }
      if (item.ivP !== null) {
        putIVData.push([item.strike, item.ivP]);
      }
      if (item.callBid !== null) {
        callBidData.push([item.strike, item.callBid]);
      }
      if (item.callAsk !== null) {
        callAskData.push([item.strike, item.callAsk]);
      }
      if (item.putBid !== null) {
        putBidData.push([item.strike, item.putBid]);
      }
      if (item.putAsk !== null) {
        putAskData.push([item.strike, item.putAsk]);
      }
    });

    return {
      callIVData,
      putIVData,
      callBidData,
      callAskData,
      putBidData,
      putAskData,
    };
  }, [smileData]);

  // Настройки ECharts
  const option = useMemo(() => {
    const series = [];

    // Call IV line
    if (chartData.callIVData.length > 0) {
      series.push({
        name: "Call IV",
        type: "line",
        data: chartData.callIVData,
        symbol: "none",
        lineStyle: {
          color: "#3b82f6",
          width: 1.5,
        },
        z: 10,
      });
    }

    // Put IV line
    if (chartData.putIVData.length > 0) {
      series.push({
        name: "Put IV",
        type: "line",
        data: chartData.putIVData,
        symbol: "none",
        lineStyle: {
          color: "#ec4899",
          width: 1.5,
        },
        z: 10,
      });
    }

    // Historical volatility reference line
    if (hv != null) {
      series.push({
        name: "Hist Vol",
        type: "line",
        markLine: {
          silent: true,
          lineStyle: {
            color: "#22d3ee",
            type: "dashed",
            width: 1,
          },
          data: [
            {
              yAxis: hv,
            },
          ],
        },
        z: 5,
      });
    }

    // Spot price reference line
    if (spotPrice != null) {
      series.push({
        name: "Spot Price",
        type: "line",
        markLine: {
          silent: true,
          lineStyle: {
            color: "#f59e0b",
            type: "dashed",
            width: 1,
          },
          data: [
            {
              xAxis: spotPrice,
            },
          ],
        },
        z: 5,
      });
    }

    // Bid/Ask scatter points
    if (chartData.callBidData.length > 0) {
      series.push({
        name: "Call Bid",
        type: "scatter",
        data: chartData.callBidData,
        symbol: "triangle",
        symbolSize: 8,
        itemStyle: {
          color: "#10b981",
        },
        z: 20,
      });
    }

    if (chartData.callAskData.length > 0) {
      series.push({
        name: "Call Ask",
        type: "scatter",
        data: chartData.callAskData,
        symbol: "triangleDown",
        symbolSize: 8,
        itemStyle: {
          color: "#10b981",
        },
        z: 20,
      });
    }

    if (chartData.putBidData.length > 0) {
      series.push({
        name: "Put Bid",
        type: "scatter",
        data: chartData.putBidData,
        symbol: "triangle",
        symbolSize: 8,
        itemStyle: {
          color: "#ef4444",
        },
        z: 20,
      });
    }

    if (chartData.putAskData.length > 0) {
      series.push({
        name: "Put Ask",
        type: "scatter",
        data: chartData.putAskData,
        symbol: "triangleDown",
        symbolSize: 8,
        itemStyle: {
          color: "#ef4444",
        },
        z: 20,
      });
    }

    return {
      animation: false,
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(15, 23, 42, 0.95)",
        borderColor: "#475569",
        textStyle: {
          color: "#cbd5e1",
        },
        axisPointer: {
          type: "cross",
        },
        formatter: function (params) {
          const strike = params[0].value[0];
          let result = `<div style="font-size: 12px; margin-bottom: 4px; color: #cbd5e1;">Strike: ${strike}</div>`;

          params.forEach((param) => {
            const color = param.color;
            const name = param.seriesName;
            const value = param.value[1];
            result += `<div style="color: ${color}; font-size: 11px;">${name}: ${value}</div>`;
          });

          return result;
        },
      },
      grid: {
        left: "3%",
        right: "3%",
        top: "3%",
        bottom: "3%",
        containLabel: true,
      },
      xAxis: {
        type: "value",
        axisLine: {
          lineStyle: {
            color: "#64748b",
          },
        },
        axisLabel: {
          color: "#64748b",
          fontSize: 10,
        },
        splitLine: {
          lineStyle: {
            color: "rgba(100, 116, 139, 0.2)",
          },
        },
      },
      yAxis: {
        type: "value",
        axisLine: {
          lineStyle: {
            color: "#64748b",
          },
        },
        axisLabel: {
          color: "#64748b",
          fontSize: 10,
        },
        splitLine: {
          lineStyle: {
            color: "rgba(100, 116, 139, 0.2)",
          },
        },
      },
      series: series,
    };
  }, [chartData, hv, spotPrice]);

  // Инициализация и обновление графика
  useEffect(() => {
    if (!chartRef.current) return;

    if (!chartInstance.current) {
      chartInstance.current = echarts.init(chartRef.current);
    }

    chartInstance.current.setOption(option);

    const handleResize = () => {
      chartInstance.current?.resize();
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chartInstance.current?.dispose();
      chartInstance.current = null;
    };
  }, [option]);

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
        <div ref={chartRef} style={{ width: "100%", height: "100%" }} />
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
