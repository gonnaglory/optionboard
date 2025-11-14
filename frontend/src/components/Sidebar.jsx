// Sidebar.jsx
import React, { useState, useEffect, memo } from "react";
import { Link, useParams } from "react-router-dom";
import { getLogo } from "../utils/getLogo";

const API_URL = import.meta.env.VITE_API_URL ?? "/api";

const Sidebar = () => {
  const { asset: currentAsset } = useParams();
  const [assets, setAssets] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    fetch(`${API_URL}/`)
      .then((response) => response.json())
      .then(setAssets)
      .catch((err) => console.error("Ошибка загрузки активов:", err));
  }, []);

  const filteredAssets = assets.filter((ticker) =>
    ticker.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <aside className="h-screen bg-slate-950 border-r border-slate-800 w-44 flex flex-col scrollbar-thin scrollbar-thumb-slate-600 scrollbar-track-transparent">
      <div className="shrink-0">
        <Link
          to="/"
          className="block text-lg font-extrabold px-3 py-3 text-slate-100 hover:text-cyan-400 transition-colors select-none"
        >
          Активы
        </Link>

        <div className="px-2 pb-2">
          <input
            type="text"
            placeholder="Поиск..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full px-2 py-1 bg-slate-900 border border-slate-700 rounded text-slate-200 placeholder-slate-500 focus:outline-none focus:border-cyan-500 transition-colors text-sm"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto scrollbar-thin scrollbar-thumb-slate-600 scrollbar-track-transparent">
        <ul className="p-1">
          {filteredAssets.map((ticker) => {
            const logo = getLogo(ticker);
            const isActive = currentAsset === ticker;

            return (
              <li key={ticker} className="mb-1">
                <Link
                  to={`/${ticker}`}
                  className={`flex items-center gap-2 px-2 py-1 rounded text-sm font-medium transition-colors ${
                    isActive
                      ? "bg-cyan-500 text-white"
                      : "text-slate-300 hover:bg-slate-800 hover:text-cyan-300"
                  }`}
                >
                  {logo && (
                    <img
                      src={logo}
                      alt={ticker}
                      className="h-4 w-4 object-contain"
                    />
                  )}
                  <span className="truncate">{ticker}</span>
                </Link>
              </li>
            );
          })}
        </ul>
      </div>
    </aside>
  );
};

export default memo(Sidebar);
