import React, { useEffect } from "react";
import { BrowserRouter as Router, Routes, Route, useLocation } from "react-router-dom";
import { AnimatePresence, motion } from "framer-motion";
import Showcase from "./components/Home";
import AssetBoard from "./components/AssetBoard";
import Sidebar from "./components/Sidebar";

const Layout = () => {
  const location = useLocation();
  const isAssetPage = location.pathname !== "/";

  // 👇 Добавляем useEffect прямо сюда
  useEffect(() => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, [location.pathname]);

  return (
    <div className="flex min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-800 text-slate-100">
      <AnimatePresence initial={false}>
        {isAssetPage && (
          <motion.div
            initial={{ width: 0, opacity: 0 }}
            animate={{ width: 260, opacity: 1 }}
            exit={{ width: 0, opacity: 0 }}
            transition={{ duration: 0.5, ease: "easeInOut" }}
            className="backdrop-blur-xl"
          >
            <Sidebar />
          </motion.div>
        )}
      </AnimatePresence>

      <motion.main
        key={location.pathname}
        initial={{ opacity: 0, x: 60 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: -60 }}
        transition={{ duration: 0.5, ease: "easeOut" }}
        className="flex-1 p-10"
      >
        <Routes location={location}>
          <Route path="/" element={<Showcase />} />
          <Route path="/:asset" element={<AssetBoard />} />
        </Routes>
      </motion.main>
    </div>
  );
};

const App = () => (
  <Router>
    <Layout />
  </Router>
);

export default App;