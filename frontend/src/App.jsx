// App.jsx
import React, { useEffect } from "react";
import {
  BrowserRouter as Router,
  Routes,
  Route,
  useLocation,
} from "react-router-dom";
import { AnimatePresence, motion } from "framer-motion";
import Showcase from "./components/Home";
import AssetBoard from "./components/AssetBoard";
import Sidebar from "./components/Sidebar";

const Layout = () => {
  const location = useLocation();
  const isAssetPage = location.pathname !== "/";

  useEffect(() => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  }, [location.pathname]);

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-100">
      <AnimatePresence initial={false}>
        {isAssetPage && (
          <motion.div
            initial={{ width: 0, opacity: 0 }}
            animate={{ width: 176, opacity: 1 }}
            exit={{ width: 0, opacity: 0 }}
            transition={{ duration: 0.2, ease: "easeOut" }}
          >
            <Sidebar />
          </motion.div>
        )}
      </AnimatePresence>

      <motion.main
        key={location.pathname}
        initial={{ opacity: 0, x: 24 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: -24 }}
        transition={{ duration: 0.2, ease: "easeOut" }}
        className="flex-1"
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
