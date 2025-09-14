import React from "react";
import { Link } from "react-router-dom";
import assets from "../../backend/data/UNDERLYINGASSETS.json";
import { getLogo } from "../utils/getLogo";

const Showcase = () => {
  return (
    <div className="p-12 animate-fade-in">
      <h1 className="text-4xl font-bold tracking-tight text-slate-100 mb-12">
        Витрина активов
      </h1>
      <div className="grid grid-cols-2 gap-8 sm:grid-cols-3 lg:grid-cols-6">
        {assets.map((ticker) => {
          const logo = getLogo(ticker);

          return (
            <Link
              key={ticker}
              to={`/${ticker}`}
              className="group block rounded-2xl bg-slate-900/60 border border-slate-700 p-6 shadow-lg hover:shadow-blue-500/30 hover:border-blue-400 transition-all duration-500 backdrop-blur-lg"
            >
              <div className="aspect-square flex items-center justify-center">
                {logo ? (
                  <img
                    src={logo}
                    alt={ticker}
                    className="h-16 w-16 object-contain transition-transform duration-500 group-hover:scale-110 group-hover:drop-shadow-[0_0_12px_rgba(59,130,246,0.7)]"
                  />
                ) : (
                  <span className="text-slate-500">{ticker}</span>
                )}
              </div>
              <div className="mt-4 text-center">
                <h3 className="text-sm font-medium text-slate-200 group-hover:text-blue-400 transition-colors">
                  {ticker}
                </h3>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
};

export default Showcase;