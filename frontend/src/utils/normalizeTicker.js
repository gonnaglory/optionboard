// Нормализация тикеров MOEX-фьючерсов
// Убираем суффикс: [буква месяца][год]
export function normalizeTicker(ticker) {
  if (!ticker) return "";
  if (ticker.length <= 2) return ticker; // защита от ошибок
  return ticker.slice(0, 2);
}