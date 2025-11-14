// AssetBoard.jsx
import React, {
  useEffect,
  useMemo,
  useState,
  useDeferredValue,
  useCallback,
  lazy,
  Suspense,
} from "react";
import { useParams } from "react-router-dom";
import { motion } from "framer-motion";

const OptionsTable = lazy(() => import("./OptionsTable"));
const CandlesChart = lazy(() => import("./CandlesChart"));
const GEX = lazy(() => import("./Gex"));
const VolatilitySmile = lazy(() => import("./VolatilitySmile"));

const API_URL = import.meta.env.VITE_API_URL ?? "/api";

function parseMaybeDate(v) {
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

function getExpiryData(data, expiry) {
  if (!data || !expiry || !data[expiry]) return null;
  return Array.isArray(data[expiry]) ? data[expiry] : null;
}

export default function AssetBoard() {
  const { asset } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedExpiry, setSelectedExpiry] = useState(null);
  const deferredExpiry = useDeferredValue(selectedExpiry);

  const [readyHeavy, setReadyHeavy] = useState(false);
  useEffect(() => {
    let mounted = true;
    let rafId = 0;

    const initHeavy = () => {
      if (mounted) {
        setReadyHeavy(true);
      }
    };

    rafId = requestAnimationFrame(() => {
      rafId = requestAnimationFrame(initHeavy);
    });

    return () => {
      mounted = false;
      if (rafId) {
        cancelAnimationFrame(rafId);
      }
    };
  }, []);

  useEffect(() => {
    let aborted = false;
    setLoading(true);
    setError(null);
    setData(null);
    setSelectedExpiry(null);

    fetch(`${API_URL}/${asset}`)
      .then((r) => {
        if (!r.ok) {
          throw new Error(`HTTP ${r.status}: ${r.statusText}`);
        }
        return r.json();
      })
      .then((json) => {
        if (aborted) return;

        if (json && typeof json === "object") {
          setData(json);
        } else {
          throw new Error("Invalid data format");
        }
      })
      .catch((e) => {
        console.error("AssetBoard: load error", e);
        if (!aborted) {
          setError(e.message);
          setData({});
        }
      })
      .finally(() => {
        if (!aborted) setLoading(false);
      });

    return () => {
      aborted = true;
    };
  }, [asset]);

  const expiries = useMemo(() => {
    if (!data || typeof data !== "object") return [];

    const keys = Object.keys(data).filter((k) => {
      const value = data[k];
      return Array.isArray(value) && value.length > 0;
    });

    if (!keys.length) return [];

    const allDates = keys.every((k) => parseMaybeDate(k) !== null);
    return keys.sort((a, b) =>
      allDates ? parseMaybeDate(a) - parseMaybeDate(b) : a.localeCompare(b)
    );
  }, [data]);

  useEffect(() => {
    if (!expiries.length) {
      setSelectedExpiry(null);
      return;
    }

    setSelectedExpiry((prev) => {
      if (prev && expiries.includes(prev)) {
        return prev;
      }
      return expiries[0];
    });
  }, [expiries]);

  const onPickExpiry = useCallback((ex) => {
    setSelectedExpiry(ex);
  }, []);

  const currentExpiryData = useMemo(() => {
    return getExpiryData(data, deferredExpiry);
  }, [data, deferredExpiry]);

  const chartHeight = 320;

  const renderContent = () => {
    if (loading) {
      return (
        <div className="rounded-lg border border-slate-700 p-3 text-slate-400 bg-slate-900/60">
          Загрузка…
        </div>
      );
    }

    if (error) {
      return (
        <div className="rounded-lg border border-red-800 p-3 text-red-400 bg-slate-900/60">
          Ошибка загрузки: {error}
        </div>
      );
    }

    if (!data || !deferredExpiry || !currentExpiryData) {
      return (
        <div className="rounded-lg border border-slate-700 p-3 text-slate-400 bg-slate-900/60">
          Нет данных для {asset}
        </div>
      );
    }

    return (
      <>
        {/* Графики */}
        <div
          className="rounded-lg border border-slate-700 bg-slate-900/60"
          style={{
            height: chartHeight,
            contentVisibility: readyHeavy ? "visible" : "auto",
          }}
        >
          {readyHeavy && (
            <Suspense
              fallback={
                <div className="p-2 text-slate-400 text-sm">
                  Загрузка графиков…
                </div>
              }
            >
              <div className="flex gap-2 items-stretch h-full">
                <div className="flex-1 min-w-0">
                  <CandlesChart asset={asset} height={chartHeight} />
                </div>
                <GEX
                  data={{ options: currentExpiryData }}
                  height={chartHeight}
                  className="w-64 shrink-0"
                />
              </div>
            </Suspense>
          )}
        </div>

        {/* Таблица опционов и Volatility Smile */}
        <div className="flex gap-2 h-[480px]">
          {/* OptionsTable - 50% ширины */}
          <div className="flex-1 min-w-0">
            <div
              className="rounded-lg border border-slate-700 bg-slate-900/60 h-full"
              style={{
                contentVisibility: readyHeavy ? "visible" : "auto",
              }}
            >
              {readyHeavy ? (
                <Suspense
                  fallback={
                    <div className="p-2 text-slate-400 text-sm">
                      Загрузка таблицы…
                    </div>
                  }
                >
                  <OptionsTable
                    data={data}
                    expiry={deferredExpiry}
                    height="100%"
                  />
                </Suspense>
              ) : (
                <div className="p-2 text-slate-400 text-sm">
                  Загрузка таблицы…
                </div>
              )}
            </div>
          </div>

          {/* Volatility Smile - 50% ширины */}
          <div className="flex-1 min-w-0">
            <div
              className="rounded-lg border border-slate-700 bg-slate-900/60 h-full"
              style={{
                contentVisibility: readyHeavy ? "visible" : "auto",
              }}
            >
              {readyHeavy ? (
                <Suspense
                  fallback={
                    <div className="p-2 text-slate-400 text-sm">
                      Загрузка Volatility Smile…
                    </div>
                  }
                >
                  <VolatilitySmile data={currentExpiryData} height="100%" />
                </Suspense>
              ) : (
                <div className="p-2 text-slate-400 text-sm">
                  Загрузка Volatility Smile…
                </div>
              )}
            </div>
          </div>
        </div>
      </>
    );
  };

  return (
    <div className="flex flex-col gap-2 p-1">
      {" "}
      {/* Еще больше уменьшены отступы */}
      {/* Заголовок */}
      <h3 className="font-semibold text-lg text-slate-100 select-none px-1">
        {asset}
      </h3>
      {/* Вкладки экспираций */}
      <div
        className="relative inline-flex flex-wrap gap-1 p-1 rounded-full border border-slate-600 bg-slate-800/80 transform-gpu"
        style={{ willChange: "transform" }}
      >
        {expiries.map((ex) => {
          const active = ex === selectedExpiry;
          const expiryData = getExpiryData(data, ex);
          const dataCount = expiryData ? expiryData.length : 0;

          return (
            <button
              key={ex}
              onClick={() => onPickExpiry(ex)}
              disabled={dataCount === 0}
              className={`relative px-2 py-1 rounded-full text-sm transition-all duration-150 select-none ${
                active
                  ? "text-white"
                  : dataCount === 0
                  ? "text-slate-500 cursor-not-allowed"
                  : "text-slate-300 hover:bg-slate-700/80"
              }`}
              title={dataCount === 0 ? "Нет данных" : `${dataCount} опционов`}
            >
              {active && readyHeavy && (
                <motion.span
                  layoutId="tab-pill"
                  transition={{
                    type: "spring",
                    stiffness: 700,
                    damping: 40,
                    mass: 0.3,
                  }}
                  className="absolute inset-0 rounded-full bg-cyan-600"
                  style={{ zIndex: 0 }}
                />
              )}
              <span className="relative z-10">
                {ex}
                {dataCount > 0 && (
                  <span className="ml-1 text-xs opacity-70">({dataCount})</span>
                )}
              </span>
            </button>
          );
        })}

        {!expiries.length && !loading && (
          <span className="text-slate-500 px-2 py-1 text-sm">
            Нет экспираций с данными
          </span>
        )}
      </div>
      {renderContent()}
    </div>
  );
}
