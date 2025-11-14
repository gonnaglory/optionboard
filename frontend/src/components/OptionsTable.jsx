// OptionsTable.jsx
import React, {
  useMemo,
  useRef,
  useState,
  useLayoutEffect,
  useCallback,
  memo,
} from "react";

function toNum(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeOption(row) {
  return {
    SECID: row.SECID,
    OPTIONTYPE: String(row.OPTIONTYPE).toUpperCase(),
    STRIKE: toNum(row.STRIKE),
    LASTTRADEDATE: row.LASTTRADEDATE,
    UNDERLYINGSETTLEPRICE: toNum(row.UNDERLYINGSETTLEPRICE),
    PREVSETTLEPRICE: toNum(row.PREVSETTLEPRICE),
    IMPLIED_VOL: toNum(row.IMPLIED_VOL),
    HIST_VOL: toNum(row.HIST_VOL),
    THEORETICAL_PRICE: toNum(row.THEORETICAL_PRICE),
    DELTA: toNum(row.DELTA),
    GAMMA: toNum(row.GAMMA),
    VEGA: toNum(row.VEGA),
    THETA: toNum(row.THETA),
  };
}

const fmt = {
  price: (v) => (v == null ? "—" : v.toFixed(2)),
  iv: (v) => (v == null ? "—" : (v * 100).toFixed(2) + "%"),
  greeks: (v) => (v == null ? "—" : v.toFixed(4)),
  strike: (v) => (v == null ? "—" : String(v)),
};

const useFormatters = () => useMemo(() => fmt, []);

function useOptionsForExpiry(data, expiry) {
  return useMemo(() => {
    if (!expiry || !data || typeof data !== "object") return [];

    if (Array.isArray(data[expiry])) return data[expiry];
    if (Array.isArray(data?.byExpiry?.[expiry])) return data.byExpiry[expiry];
    if (Array.isArray(data?.groupedByExpiry?.[expiry]))
      return data.groupedByExpiry[expiry];
    if (Array.isArray(data?.options)) {
      return data.options.filter((row) => {
        const rowExpiry = row.LASTTRADEDATE ?? row.expiry ?? row.expiration;
        return String(rowExpiry) === String(expiry);
      });
    }

    return [];
  }, [data, expiry]);
}

function useGroupedOptions(options) {
  return useMemo(() => {
    if (!Array.isArray(options) || options.length === 0) {
      return { strikes: [], grouped: new Map() };
    }

    const grouped = new Map();
    const strikesSet = new Set();

    for (let i = 0; i < options.length; i++) {
      const normalized = normalizeOption(options[i]);
      const strike = normalized.STRIKE;

      if (!Number.isFinite(strike)) continue;

      strikesSet.add(strike);

      if (!grouped.has(strike)) {
        grouped.set(strike, { C: null, P: null });
      }

      const bucket = grouped.get(strike);
      const type = normalized.OPTIONTYPE;
      if (type === "C" || type === "P") {
        bucket[type] = normalized;
      }
    }

    const strikes = Array.from(strikesSet).sort((a, b) => b - a);

    return { strikes, grouped };
  }, [options]);
}

const OptionsTable = memo(function OptionsTable({
  data,
  expiry,
  height = 480,
  rowHeight = 36,
  overscan = 4,
}) {
  const options = useOptionsForExpiry(data, expiry);
  const { strikes, grouped } = useGroupedOptions(options);
  const formatters = useFormatters();

  const total = strikes.length;
  const containerRef = useRef(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewHeight, setViewHeight] = useState(height);
  const rafRef = useRef(0);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const updateHeight = () => {
      setViewHeight(el.clientHeight);
    };

    const ro = new ResizeObserver((entries) => {
      if (entries[0]) {
        setViewHeight(entries[0].contentRect.height);
      }
    });

    ro.observe(el);
    updateHeight();

    return () => {
      ro.disconnect();
      if (rafRef.current) {
        cancelAnimationFrame(rafRef.current);
      }
    };
  }, []);

  const onScroll = useCallback((e) => {
    if (rafRef.current) {
      cancelAnimationFrame(rafRef.current);
    }

    rafRef.current = requestAnimationFrame(() => {
      setScrollTop(e.currentTarget.scrollTop);
    });
  }, []);

  const visibleRange = useMemo(() => {
    const startIndex = Math.max(
      0,
      Math.floor(scrollTop / rowHeight) - overscan
    );
    const visibleRowCount = Math.ceil(viewHeight / rowHeight);
    const endIndex = Math.min(
      total,
      startIndex + visibleRowCount + overscan * 2
    );

    const topPad = startIndex * rowHeight;
    const bottomPad = Math.max(0, total * rowHeight - endIndex * rowHeight);

    return {
      startIndex,
      endIndex,
      topPad,
      bottomPad,
    };
  }, [scrollTop, viewHeight, rowHeight, overscan, total]);

  const { startIndex, endIndex, topPad, bottomPad } = visibleRange;

  const renderRow = useCallback(
    (strike) => {
      const bucket = grouped.get(strike);
      const C = bucket?.C;
      const P = bucket?.P;

      const gamma = C?.GAMMA ?? P?.GAMMA;
      const vega = C?.VEGA ?? P?.VEGA;
      const theta = C?.THETA ?? P?.THETA;

      return (
        <div
          key={strike}
          className="grid grid-cols-10 items-center px-2 border-b border-slate-700 last:border-b-0 hover:bg-slate-800/60 transition-colors duration-100 text-xs"
          style={{ height: rowHeight }}
        >
          <div className="text-right text-slate-300">
            {formatters.greeks(C?.DELTA)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.iv(C?.IMPLIED_VOL)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.price(C?.THEORETICAL_PRICE)}
          </div>

          <div className="font-mono text-center font-semibold text-slate-100">
            {formatters.strike(strike)}
          </div>

          <div className="text-right text-slate-300">
            {formatters.price(P?.THEORETICAL_PRICE)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.iv(P?.IMPLIED_VOL)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.greeks(P?.DELTA)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.greeks(gamma)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.greeks(vega)}
          </div>
          <div className="text-right text-slate-300">
            {formatters.greeks(theta)}
          </div>
        </div>
      );
    },
    [grouped, formatters, rowHeight]
  );

  if (!expiry) {
    return (
      <div className="rounded-lg border border-slate-700 bg-slate-900/60">
        <div className="p-4 text-center text-slate-400 text-sm">
          Выберите экспирацию
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-slate-700 bg-slate-900/60 overflow-hidden">
      <div className="px-3 py-2 border-b border-slate-700 bg-slate-800/80 flex items-baseline gap-2 select-none">
        <h3 className="font-semibold text-slate-100 text-sm">
          Опционы · {expiry}
        </h3>
        <span className="text-xs text-slate-400">({total} страйков)</span>
      </div>

      <div className="grid grid-cols-10 text-xs bg-slate-800 text-slate-300 px-2 py-1 font-medium">
        <div className="text-right">Delta C</div>
        <div className="text-right">Call IV</div>
        <div className="text-right">Call Theo</div>
        <div className="text-center">Strike</div>
        <div className="text-right">Put Theo</div>
        <div className="text-right">Put IV</div>
        <div className="text-right">Delta P</div>
        <div className="text-right">Gamma</div>
        <div className="text-right">Vega</div>
        <div className="text-right">Theta</div>
      </div>

      <div
        ref={containerRef}
        onScroll={onScroll}
        className="relative overflow-auto will-change-transform transform-gpu"
        style={{ height }}
      >
        {total === 0 ? (
          <div className="flex items-center justify-center h-full text-slate-400 text-sm">
            Нет данных
          </div>
        ) : (
          <>
            {topPad > 0 && <div style={{ height: topPad }} />}
            <div>{strikes.slice(startIndex, endIndex).map(renderRow)}</div>
            {bottomPad > 0 && <div style={{ height: bottomPad }} />}
          </>
        )}
      </div>
    </div>
  );
});

OptionsTable.displayName = "OptionsTable";

export default OptionsTable;
