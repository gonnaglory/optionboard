import { normalizeTicker } from "./normalizeTicker";

// logos импортируется через import.meta.glob (у тебя уже есть)
const logos = import.meta.glob("/src/assets/*.png", { eager: true });

export function getLogo(ticker) {
  const baseTicker = normalizeTicker(ticker);
  const path = `/src/assets/${baseTicker}.png`;
  return logos[path]?.default || null;
}