import React from "react";
import { Link, useParams } from "react-router-dom";
import { motion } from "framer-motion";
import assets from "../../backend/data/UNDERLYINGASSETS.json";
import { getLogo } from "../utils/getLogo";

const Sidebar = () => {
  const { asset } = useParams();

  return (
    <motion.aside
      initial={{ x: -50, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      exit={{ x: -50, opacity: 0 }}
      transition={{ duration: 0.5, ease: "easeInOut" }}
      className="h-screen overflow-y-auto bg-slate-950/90 backdrop-blur-xl shadow-2xl border-r border-slate-800
                 scrollbar-thin scrollbar-thumb-slate-700 scrollbar-track-slate-900"
    >
      <div className="sticky top-0 bg-slate-950/95 backdrop-blur-xl border-b border-slate-800 z-10">
        <Link
          to="/"
          className="block text-xl font-extrabold px-6 py-4 text-neon-blue tracking-wide
                     hover:text-neon-pink transition-colors duration-300"
        >
          Активы
        </Link>
      </div>

      <ul className="space-y-2 p-4">
        {assets.map((ticker) => {
          const logo = getLogo(ticker);
          const isActive = asset === ticker;
          return (
            <li key={ticker}>
              <Link
                to={`/${ticker}`}
                className={`flex items-center gap-3 px-4 py-2 rounded-xl font-medium transition-all duration-300 ${
                  isActive
                    ? "bg-slate-900/60 text-neon-blue shadow-[0_0_15px_rgba(59,130,246,0.8)]"
                    : "text-slate-300 hover:bg-slate-800 hover:text-neon-pink hover:shadow-[0_0_12px_rgba(236,72,153,0.6)]"
                }`}
              >
                {logo && (
                  <img
                    src={logo}
                    alt={ticker}
                    className="h-5 w-5 object-contain"
                  />
                )}
                {ticker}
              </Link>
            </li>
          );
        })}
      </ul>
    </motion.aside>
  );
};

export default Sidebar;