import numpy as np
from scipy.stats import norm
from numba import jit, float64, int64, types, prange, vectorize
import math
from typing import List, Tuple, Dict, Any
import asyncio

from backend.config import settings

# ---------------- Numba-УСКОРЕННЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------------

@jit(float64(float64), nopython=True)
def norm_cdf_numba(x):
    """Numba-совместимая функция CDF"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@jit(float64(float64), nopython=True)
def norm_pdf_numba(x):
    """Numba-совместимая функция PDF"""
    return math.exp(-0.5 * x**2) / math.sqrt(2.0 * math.pi)

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:], int64[:]), 
     nopython=True, parallel=True)
def calculate_black76_numba(F0_array, K_array, T_years_array, r_array, sigma_array, option_type_int_array):
    """Numba-ускоренный расчет цен Black-76"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 0 or sigma_array[i] <= 0 or 
            F0_array[i] <= 0 or K_array[i] <= 0):
            result[i] = 0.0
            continue
            
        d1 = (math.log(F0_array[i] / K_array[i]) + 
              0.5 * sigma_array[i]**2 * T_years_array[i]) / \
             (sigma_array[i] * math.sqrt(T_years_array[i]))
        d2 = d1 - sigma_array[i] * math.sqrt(T_years_array[i])
        discount = math.exp(-r_array[i] * T_years_array[i])
        
        if option_type_int_array[i] == 0:  # Call
            result[i] = discount * (F0_array[i] * norm_cdf_numba(d1) - K_array[i] * norm_cdf_numba(d2))
        else:  # Put
            result[i] = discount * (K_array[i] * norm_cdf_numba(-d2) - F0_array[i] * norm_cdf_numba(-d1))
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:], int64[:]), 
     nopython=True, parallel=True)
def calculate_delta_numba(F0_array, K_array, T_years_array, r_array, sigma_array, option_type_int_array):
    """Numba-ускоренный расчет Delta"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 0 or sigma_array[i] <= 0 or 
            F0_array[i] <= 0 or K_array[i] <= 0):
            result[i] = 0.0
            continue
            
        d1 = (math.log(F0_array[i] / K_array[i]) + 
              0.5 * sigma_array[i]**2 * T_years_array[i]) / \
             (sigma_array[i] * math.sqrt(T_years_array[i]))
        discount = math.exp(-r_array[i] * T_years_array[i])
        
        if option_type_int_array[i] == 0:  # Call
            result[i] = discount * norm_cdf_numba(d1)
        else:  # Put
            result[i] = -discount * norm_cdf_numba(-d1)
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:]), 
     nopython=True, parallel=True)
def calculate_gamma_numba(F0_array, K_array, T_years_array, r_array, sigma_array):
    """Numba-ускоренный расчет Gamma"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 0 or sigma_array[i] <= 0 or 
            F0_array[i] <= 0 or K_array[i] <= 0):
            result[i] = 0.0
            continue
            
        d1 = (math.log(F0_array[i] / K_array[i]) + 
              0.5 * sigma_array[i]**2 * T_years_array[i]) / \
             (sigma_array[i] * math.sqrt(T_years_array[i]))
        discount = math.exp(-r_array[i] * T_years_array[i])
        result[i] = discount * norm_pdf_numba(d1) / (F0_array[i] * sigma_array[i] * math.sqrt(T_years_array[i]))
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:]), 
     nopython=True, parallel=True)
def calculate_vega_numba(F0_array, K_array, T_years_array, r_array, sigma_array):
    """Numba-ускоренный расчет Vega"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 0 or sigma_array[i] <= 0 or 
            F0_array[i] <= 0 or K_array[i] <= 0):
            result[i] = 0.0
            continue
            
        d1 = (math.log(F0_array[i] / K_array[i]) + 
              0.5 * sigma_array[i]**2 * T_years_array[i]) / \
             (sigma_array[i] * math.sqrt(T_years_array[i]))
        discount = math.exp(-r_array[i] * T_years_array[i])
        result[i] = (discount * F0_array[i] * norm_pdf_numba(d1) * math.sqrt(T_years_array[i])) / 100
    
    return result

# ---------------- ОСНОВНАЯ ВЕКТОРИЗОВАННАЯ ФУНКЦИЯ С NUMBA ----------------

def calculate_all_options_params_numba(
    options_data: List[Dict[str, Any]],
    hist_vol_value: float
) -> List[Dict[str, Any]]:
    """
    Векторизованный расчет всех параметров с Numba-ускорением
    """
    from .services import expiry_time
    
    n = len(options_data)
    if n == 0:
        return options_data
    
    # Подготавливаем массивы для векторизации
    F0_array = np.empty(n, dtype=np.float64)
    K_array = np.empty(n, dtype=np.float64)
    T_minutes_array = np.empty(n, dtype=np.float64)
    r_array = np.empty(n, dtype=np.float64)
    sigma_array = np.empty(n, dtype=np.float64)
    option_type_int_array = np.empty(n, dtype=np.int64)  # 0 для Call, 1 для Put
    secid_array = []
    
    # Заполняем массивы данными из опционов
    for i, option in enumerate(options_data):
        F0_array[i] = option.get('UNDERLYINGSETTLEPRICE', 0)
        K_array[i] = option.get('STRIKE', 0)
        
        # Рассчитываем время до экспирации
        expiry_date = option.get('LASTTRADEDATE')
        if expiry_date:
            T_minutes_array[i] = expiry_time(expiry_date)
        else:
            T_minutes_array[i] = 0
        
        r_array[i] = settings.IV_RATE
        sigma_array[i] = hist_vol_value
        option_type_int_array[i] = 0 if option.get('OPTIONTYPE', 'C') == 'C' else 1
        secid_array.append(option.get('SECID', ''))
    
    # Преобразуем минуты в годы
    T_years_array = T_minutes_array / (settings.TRADING_DAYS_PER_YEAR * settings.MINUTES_PER_DAY)
    
    # Маска валидных значений
    valid_mask = (T_years_array > 0) & (sigma_array > 0) & (F0_array > 0) & (K_array > 0)
    
    # Применяем маску
    F0_valid = F0_array[valid_mask]
    K_valid = K_array[valid_mask]
    T_years_valid = T_years_array[valid_mask]
    r_valid = r_array[valid_mask]
    sigma_valid = sigma_array[valid_mask]
    option_type_int_valid = option_type_int_array[valid_mask]
    
    # Инициализируем массивы результатов
    theoretical_price_array = np.full(n, np.nan, dtype=np.float64)
    delta_array = np.full(n, np.nan, dtype=np.float64)
    gamma_array = np.full(n, np.nan, dtype=np.float64)
    vega_array = np.full(n, np.nan, dtype=np.float64)
    theta_array = np.full(n, np.nan, dtype=np.float64)
    
    # Рассчитываем только для валидных опционов с Numba
    if len(F0_valid) > 0:
        # Расчет всех параметров
        price_valid = calculate_black76_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid, option_type_int_valid)
        delta_valid = calculate_delta_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid, option_type_int_valid)
        gamma_valid = calculate_gamma_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid)
        vega_valid = calculate_vega_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid)
        
        # Theta пока рассчитываем традиционным способом (сложнее для векторизации)
        for i, idx in enumerate(np.where(valid_mask)[0]):
            if option_type_int_array[idx] == 0:  # Call
                d1 = (math.log(F0_valid[i] / K_valid[i]) + 
                      0.5 * sigma_valid[i]**2 * T_years_valid[i]) / \
                     (sigma_valid[i] * math.sqrt(T_years_valid[i]))
                d2 = d1 - sigma_valid[i] * math.sqrt(T_years_valid[i])
                discount = math.exp(-r_valid[i] * T_years_valid[i])
                
                term1 = -discount * F0_valid[i] * norm_pdf_numba(d1) * sigma_valid[i] / (2 * math.sqrt(T_years_valid[i]))
                term2 = -r_valid[i] * discount * (F0_valid[i] * norm_cdf_numba(d1) - K_valid[i] * norm_cdf_numba(d2))
                theta_valid = (term1 + term2) / settings.TRADING_DAYS_PER_YEAR
            else:  # Put
                d1 = (math.log(F0_valid[i] / K_valid[i]) + 
                      0.5 * sigma_valid[i]**2 * T_years_valid[i]) / \
                     (sigma_valid[i] * math.sqrt(T_years_valid[i]))
                d2 = d1 - sigma_valid[i] * math.sqrt(T_years_valid[i])
                discount = math.exp(-r_valid[i] * T_years_valid[i])
                
                term1 = -discount * F0_valid[i] * norm_pdf_numba(d1) * sigma_valid[i] / (2 * math.sqrt(T_years_valid[i]))
                term2 = -r_valid[i] * discount * (-F0_valid[i] * norm_cdf_numba(-d1) + K_valid[i] * norm_cdf_numba(-d2))
                theta_valid = (term1 + term2) / settings.TRADING_DAYS_PER_YEAR
            
            theta_array[idx] = theta_valid
        
        # Заполняем результаты
        theoretical_price_array[valid_mask] = price_valid
        delta_array[valid_mask] = delta_valid
        gamma_array[valid_mask] = gamma_valid
        vega_array[valid_mask] = vega_valid
    
    # Обновляем опционы с результатами
    for i, option in enumerate(options_data):
        option['HIST_VOL'] = hist_vol_value
        option['IMPLIED_VOL'] = round(float(hist_vol_value), 4)
        option['THEORETICAL_PRICE'] = round(float(theoretical_price_array[i]), 2) if not np.isnan(theoretical_price_array[i]) else None
        option['DELTA'] = round(float(delta_array[i]), 6) if not np.isnan(delta_array[i]) else None
        option['GAMMA'] = round(float(gamma_array[i]), 6) if not np.isnan(gamma_array[i]) else None
        option['VEGA'] = round(float(vega_array[i]), 6) if not np.isnan(vega_array[i]) else None
        option['THETA'] = round(float(theta_array[i]), 6) if not np.isnan(theta_array[i]) else None
    
    return options_data

# ---------------- АСИНХРОННАЯ ОБРАБОТКА ----------------

async def process_asset_options(options_data: List[dict], hist_vol_value: float) -> List[dict]:
    """
    Асинхронно обрабатывает все опционы актива с Numba-ускорением
    """
    # Запускаем в отдельном потоке чтобы не блокировать event loop
    loop = asyncio.get_event_loop()
    processed_data = await loop.run_in_executor(
        None, 
        calculate_all_options_params_numba, 
        options_data, 
        hist_vol_value
    )
    return processed_data