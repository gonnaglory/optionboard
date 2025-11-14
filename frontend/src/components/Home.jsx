import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { getLogo } from "../utils/getLogo";

const API_URL = import.meta.env.VITE_API_URL ?? "/api";

const Showcase = () => {
  const [assets, setAssets] = useState([]);

  useEffect(() => {
    fetch(`${API_URL}/`)
      .then((response) => response.json())
      .then(setAssets)
      .catch((err) => console.error("Ошибка загрузки активов:", err));
  }, []);

  return (
    <div className="p-3 animate-fade-in">
      <div className="w-full">
        <h1 className="text-2xl font-bold bg-linear-to-r from-slate-100 to-slate-300 bg-clip-text text-transparent mb-6 text-center">
          Витрина активов
        </h1>

        <div className="grid grid-cols-4 gap-3 sm:grid-cols-5 lg:grid-cols-7 xl:grid-cols-8 2xl:grid-cols-10 w-full">
          {assets.map((ticker) => {
            const logo = getLogo(ticker);

            return (
              <Link
                key={ticker}
                to={`/${ticker}`}
                className="group block rounded-xl bg-slate-800/80 p-3 shadow-md hover:shadow-blue-500/10 transition-all duration-200 hover:bg-slate-700/80 border border-slate-700/50 hover:border-blue-400/50"
              >
                <div className="aspect-square flex items-center justify-center">
                  {logo ? (
                    <img
                      src={logo}
                      alt={ticker}
                      className="h-8 w-8 object-contain transition-transform duration-200 group-hover:scale-110"
                    />
                  ) : (
                    <span className="text-slate-400 font-medium text-xs">
                      {ticker}
                    </span>
                  )}
                </div>

                <div className="mt-2 text-center">
                  <h3 className="text-xs font-medium text-slate-200 group-hover:text-blue-300 transition-colors duration-200">
                    {ticker}
                  </h3>
                </div>
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default Showcase;
