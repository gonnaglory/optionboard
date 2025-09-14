import React, { useMemo } from "react";
import { useParams } from "react-router-dom";
import { getLogo } from "../utils/getLogo";
import VolatilitySmile from "./VolatilitySmile";
import OptionsTable from "./OptionsTable";

const allData = import.meta.glob("../../backend/data/*.json", { eager: true });

const AssetBoard = () => {
  const { asset } = useParams();

  // грузим данные
  const data = useMemo(() => {
    const path = `../../backend/data/${asset}.json`;
    const mod = allData[path];
    if (!mod) return [];
    return mod.default;
  }, [asset]);

  if (!data || data.length === 0) {
    return <p className="text-slate-400 p-8">Нет данных для {asset}</p>;
  }

  // теперь уже можно брать поля из data
  const underlyingPrice = Number(data[0]?.UNDERLYINGSETTLEPRICE) || null;
  const histVol = Number(data[0]?.HIST_VOL) || null;

  // группировка по страйкам
  const grouped = {};
  data.forEach((opt) => {
    if (!grouped[opt.STRIKE]) grouped[opt.STRIKE] = { C: {}, P: {} };
    grouped[opt.STRIKE][opt.OPTIONTYPE] = opt;
  });

  const strikes = Object.keys(grouped)
    .map((s) => Number(s))
    .sort((a, b) => b - a); // убывание

  const logo = getLogo(asset);

  return (
    <div className="animate-fade-in flex flex-col lg:flex-row gap-6">
      <VolatilitySmile
        grouped={grouped}
        strikes={strikes}
        underlyingPrice={underlyingPrice}
        histVol={histVol}
      />

      <OptionsTable
        asset={asset}
        logo={logo}
        grouped={grouped}
        strikes={strikes}
        underlyingPrice={underlyingPrice}
      />
    </div>
  );
};

export default AssetBoard;