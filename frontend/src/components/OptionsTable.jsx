import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

const OptionsTable = ({ asset, logo, grouped, strikes, underlyingPrice }) => {
  const [expandedRow, setExpandedRow] = useState(null);

  return (
    <div className="flex-1 bg-slate-950/70 rounded-2xl shadow-xl border border-slate-800 backdrop-blur-xl p-4">
      <div className="flex items-center gap-3 mb-4">
        {logo && (
          <img
            src={logo}
            alt={asset}
            className="h-8 w-8 object-contain drop-shadow-[0_0_6px_rgba(59,130,246,0.7)]"
          />
        )}
        <h2 className="text-2xl font-bold text-neon-blue">
          {asset} — Опционная доска
        </h2>
      </div>

      <div className="overflow-hidden rounded-xl">
        <table className="w-full text-sm">
          <thead className="bg-slate-800/80">
            <tr>
              <th className="px-3 py-2 text-left">Call Ask</th>
              <th className="px-3 py-2 text-left">Call Theor</th>
              <th className="px-3 py-2 text-left">Call Bid</th>
              <th className="px-3 py-2 text-center">Strike</th>
              <th className="px-3 py-2 text-right">Put Ask</th>
              <th className="px-3 py-2 text-right">Put Theor</th>
              <th className="px-3 py-2 text-right">Put Bid</th>
            </tr>
          </thead>
          <tbody>
            {strikes.map((strike, idx) => {
              const row = grouped[strike];
              const isExpanded = expandedRow === strike;

              const callITM = underlyingPrice && strike < underlyingPrice;
              const putITM = underlyingPrice && strike > underlyingPrice;

              return (
                <React.Fragment key={strike}>
                  {/* Разделитель цены базового актива */}
                  {underlyingPrice &&
                    idx > 0 &&
                    underlyingPrice < strikes[idx - 1] &&
                    underlyingPrice >= strike && (
                      <tr>
                        <td colSpan={7} className="text-center py-2">
                          <div className="relative">
                            <div className="absolute inset-0 border-t border-blue-500" />
                            <span className="relative bg-slate-900/80 px-3 text-sm text-blue-400 font-semibold rounded">
                              Цена базового актива: {underlyingPrice}
                            </span>
                          </div>
                        </td>
                      </tr>
                    )}

                  {/* Строка опциона */}
                  <tr
                    onClick={() =>
                      setExpandedRow(isExpanded ? null : strike)
                    }
                    className="cursor-pointer transition-colors hover:bg-slate-800/60"
                  >
                    <td
                      className={`px-3 py-2 ${
                        callITM ? "text-green-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.C?.ASK || "-"}
                    </td>
                    <td
                      className={`px-3 py-2 ${
                        callITM ? "text-green-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.C?.THEORETICAL_PRICE || "-"}
                    </td>
                    <td
                      className={`px-3 py-2 ${
                        callITM ? "text-green-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.C?.BID || "-"}
                    </td>
                    <td className="px-3 py-2 text-center">{strike}</td>
                    <td
                      className={`px-3 py-2 text-right ${
                        putITM ? "text-pink-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.P?.ASK || "-"}
                    </td>
                    <td
                      className={`px-3 py-2 text-right ${
                        putITM ? "text-pink-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.P?.THEORETICAL_PRICE || "-"}
                    </td>
                    <td
                      className={`px-3 py-2 text-right ${
                        putITM ? "text-pink-400 font-semibold" : "text-slate-200"
                      }`}
                    >
                      {row.P?.BID || "-"}
                    </td>
                  </tr>

                  {/* Подстрока с греками */}
                  <AnimatePresence>
                    {isExpanded && (
                      <motion.tr
                        initial={{ opacity: 0, height: 0 }}
                        animate={{ opacity: 1, height: "auto" }}
                        exit={{ opacity: 0, height: 0 }}
                        className="bg-slate-900/50"
                      >
                        <td colSpan={7} className="px-4 py-2">
                          <div className="flex flex-wrap gap-4 text-xs text-slate-300">
                            <span>Δ (C): {row.C?.DELTA || "-"}</span>
                            <span>Γ (C): {row.C?.GAMMA || "-"}</span>
                            <span>Vega (C): {row.C?.VEGA || "-"}</span>
                            <span>Theta (C): {row.C?.THETA || "-"}</span>
                            <span>Δ (P): {row.P?.DELTA || "-"}</span>
                            <span>Γ (P): {row.P?.GAMMA || "-"}</span>
                            <span>Vega (P): {row.P?.VEGA || "-"}</span>
                            <span>Theta (P): {row.P?.THETA || "-"}</span>
                          </div>
                        </td>
                      </motion.tr>
                    )}
                  </AnimatePresence>
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default OptionsTable;
