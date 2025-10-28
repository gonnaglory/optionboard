import React, { useEffect, useState, useMemo } from "react";
import { useParams } from "react-router-dom";
import { getLogo } from "../utils/getLogo";
import VolatilitySmile from "./VolatilitySmile";
import OptionsTable from "./OptionsTable";

const API_URL = import.meta.env.VITE_API_URL ?? "/api";

const AssetBoard = () => {
  const { asset } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`${API_URL}/${asset}`)
      .then((res) => res.json())
      .then((json) => setData(json))
      .catch((e) => console.error("Failed to load asset", e))
      .finally(() => setLoading(false));
  }, [asset]);

  if (loading) return <p className="p-8 text-slate-400">Загрузка…</p>;
  if (!data || data.length === 0) return <p className="p-8 text-slate-400">Нет данных для {asset}</p>;

  const underlyingPrice = Number(data[0]?.UNDERLYINGSETTLEPRICE) || null;
  const histVol = Number(data[0]?.HIST_VOL) || null;

  const grouped = {};
  data.forEach((opt) => {
    if (!grouped[opt.STRIKE]) grouped[opt.STRIKE] = { C: {}, P: {} };
    grouped[opt.STRIKE][opt.OPTIONTYPE] = opt;
  });
  const strikes = Object.keys(grouped).map(Number).sort((a, b) => b - a);

  const logo = getLogo(asset);

  return (
    <div className="animate-fade-in flex flex-col lg:flex-row gap-6">
      <VolatilitySmile grouped={grouped} strikes={strikes} underlyingPrice={underlyingPrice} histVol={histVol} />
      <OptionsTable asset={asset} logo={logo} grouped={grouped} strikes={strikes} underlyingPrice={underlyingPrice} />
    </div>
  );
};

export default AssetBoard;