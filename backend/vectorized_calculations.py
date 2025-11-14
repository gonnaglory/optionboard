import numpy as np
from scipy.stats import norm
from numba import jit, float64, int64, prange, types
import math
from typing import List, Dict, Any
import asyncio
from backend.config import settings

# ---------------- Numba-УСКОРЕННЫЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------------

@jit(float64(float64), nopython=True, cache=True)
def norm_cdf_numba(x):
    """Numba-совместимая функция CDF"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@jit(float64(float64), nopython=True, cache=True)
def norm_pdf_numba(x):
    """Numba-совместимая функция PDF"""
    return math.exp(-0.5 * x**2) / math.sqrt(2.0 * math.pi)

@jit(types.Tuple((float64[:], float64[:]))(float64[:], float64[:], float64[:], float64[:]), 
     nopython=True, parallel=True, cache=True)
def calculate_d1_d2_numba(F0_array, K_array, T_years_array, sigma_array):
    """Предрасчет d1 и d2 для всех опционов"""
    n = len(F0_array)
    d1_array = np.empty(n, dtype=np.float64)
    d2_array = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or sigma_array[i] <= 1e-10 or 
            F0_array[i] <= 1e-10 or K_array[i] <= 1e-10):
            d1_array[i] = 0.0
            d2_array[i] = 0.0
            continue
            
        sqrt_T = math.sqrt(T_years_array[i])
        if sqrt_T <= 1e-10:
            d1_array[i] = 0.0
            d2_array[i] = 0.0
            continue
            
        d1_array[i] = (math.log(F0_array[i] / K_array[i]) + 
                       0.5 * sigma_array[i]**2 * T_years_array[i]) / (sigma_array[i] * sqrt_T)
        d2_array[i] = d1_array[i] - sigma_array[i] * sqrt_T
    
    return d1_array, d2_array

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:], int64[:], float64[:], float64[:]), 
     nopython=True, parallel=True, cache=True)
def calculate_black76_numba(F0_array, K_array, T_years_array, r_array, sigma_array, option_type_int_array, d1_array, d2_array):
    """Numba-ускоренный расчет цен Black-76 с предрасчитанными d1, d2"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or sigma_array[i] <= 1e-10 or 
            F0_array[i] <= 1e-10 or K_array[i] <= 1e-10):
            # Для невалидных параметров возвращаем внутреннюю стоимость
            if option_type_int_array[i] == 0:  # Call
                result[i] = max(F0_array[i] - K_array[i], 0.0)
            else:  # Put
                result[i] = max(K_array[i] - F0_array[i], 0.0)
            continue
            
        discount = math.exp(-r_array[i] * T_years_array[i])
        
        if option_type_int_array[i] == 0:  # Call
            result[i] = discount * (F0_array[i] * norm_cdf_numba(d1_array[i]) - K_array[i] * norm_cdf_numba(d2_array[i]))
        else:  # Put
            result[i] = discount * (K_array[i] * norm_cdf_numba(-d2_array[i]) - F0_array[i] * norm_cdf_numba(-d1_array[i]))
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], int64[:], float64[:]), 
     nopython=True, parallel=True, cache=True)
def calculate_delta_numba(F0_array, T_years_array, r_array, option_type_int_array, d1_array):
    """Numba-ускоренный расчет Delta с предрасчитанными d1"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or F0_array[i] <= 1e-10):
            # Для опционов с нулевым временем до экспирации
            if option_type_int_array[i] == 0:  # Call
                result[i] = 1.0 if F0_array[i] > 0 else 0.0
            else:  # Put
                result[i] = -1.0 if F0_array[i] > 0 else 0.0
            continue
            
        discount = math.exp(-r_array[i] * T_years_array[i])
        
        if option_type_int_array[i] == 0:  # Call
            result[i] = discount * norm_cdf_numba(d1_array[i])
        else:  # Put
            result[i] = -discount * norm_cdf_numba(-d1_array[i])
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:]), 
     nopython=True, parallel=True, cache=True)
def calculate_gamma_numba(F0_array, T_years_array, r_array, sigma_array, d1_array):
    """Numba-ускоренный расчет Gamma с предрасчитанными d1"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or sigma_array[i] <= 1e-10 or 
            F0_array[i] <= 1e-10):
            result[i] = 0.0
            continue
            
        sqrt_T = math.sqrt(T_years_array[i])
        if sqrt_T <= 1e-10:
            result[i] = 0.0
            continue
            
        discount = math.exp(-r_array[i] * T_years_array[i])
        result[i] = discount * norm_pdf_numba(d1_array[i]) / (F0_array[i] * sigma_array[i] * sqrt_T)
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:]), 
     nopython=True, parallel=True, cache=True)
def calculate_vega_numba(F0_array, T_years_array, r_array, d1_array):
    """Numba-ускоренный расчет Vega с предрасчитанными d1"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or F0_array[i] <= 1e-10):
            result[i] = 0.0
            continue
            
        sqrt_T = math.sqrt(T_years_array[i])
        if sqrt_T <= 1e-10:
            result[i] = 0.0
            continue
            
        discount = math.exp(-r_array[i] * T_years_array[i])
        result[i] = discount * F0_array[i] * norm_pdf_numba(d1_array[i]) * sqrt_T / 100.0
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], float64[:], float64[:], int64[:], float64[:], float64[:], float64), 
     nopython=True, parallel=True, cache=True)
def calculate_theta_numba(F0_array, K_array, T_years_array, r_array, sigma_array, option_type_int_array, d1_array, d2_array, trading_days_per_year):
    """Numba-ускоренный расчет Theta с предрасчитанными d1, d2"""
    n = len(F0_array)
    result = np.empty(n, dtype=np.float64)
    
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or sigma_array[i] <= 1e-10 or 
            F0_array[i] <= 1e-10 or K_array[i] <= 1e-10):
            result[i] = 0.0
            continue
            
        sqrt_T = math.sqrt(T_years_array[i])
        if sqrt_T <= 1e-10:
            result[i] = 0.0
            continue
            
        discount = math.exp(-r_array[i] * T_years_array[i])
        pdf_d1 = norm_pdf_numba(d1_array[i])
        
        if option_type_int_array[i] == 0:  # Call
            term1 = -discount * F0_array[i] * pdf_d1 * sigma_array[i] / (2.0 * sqrt_T)
            term2 = -r_array[i] * discount * (F0_array[i] * norm_cdf_numba(d1_array[i]) - K_array[i] * norm_cdf_numba(d2_array[i]))
            theta = term1 + term2
        else:  # Put
            term1 = -discount * F0_array[i] * pdf_d1 * sigma_array[i] / (2.0 * sqrt_T)
            term2 = -r_array[i] * discount * (-F0_array[i] * norm_cdf_numba(-d1_array[i]) + K_array[i] * norm_cdf_numba(-d2_array[i]))
            theta = term1 + term2
        
        result[i] = theta / trading_days_per_year
    
    return result

@jit(float64[:](float64[:], float64[:], float64[:], int64[:], float64[:], float64), 
     nopython=True, parallel=True, cache=True)
def calculate_gex_numba(F0_array, T_years_array, gamma_array, option_type_int_array, oi_array, multiplier):
    """
    GEX = sign * gamma * F^2 * OI * multiplier
    sign = +1 для Call, -1 для Put
    """
    n = len(gamma_array)
    result = np.empty(n, dtype=np.float64)
    for i in prange(n):
        if (T_years_array[i] <= 1e-10 or F0_array[i] <= 1e-10 or 
            gamma_array[i] <= -1e10 or oi_array[i] <= -1e10):
            result[i] = 0.0
            continue
        sign = 1.0 if option_type_int_array[i] == 0 else -1.0
        result[i] = sign * gamma_array[i] * (F0_array[i] * F0_array[i]) * oi_array[i] * multiplier
    return result

# ---------------- ОСНОВНАЯ ВЕКТОРИЗОВАННАЯ ФУНКЦИЯ С NUMBA ----------------

def calculate_all_options_params_numba(
    options_data: List[Dict[str, Any]],
    hist_vol_value: float
) -> List[Dict[str, Any]]:
    """
    Векторизованный расчет всех параметров с Numba-ускорением и предрасчетом d1, d2
    """
    # Ленинный импорт для избежания циклических зависимостей
    from backend.services import expiry_time
    
    n = len(options_data)
    if n == 0:
        return options_data
    
    try:
        # Подготавливаем массивы для векторизации
        F0_array = np.empty(n, dtype=np.float64)
        K_array = np.empty(n, dtype=np.float64)
        T_minutes_array = np.empty(n, dtype=np.float64)
        r_array = np.empty(n, dtype=np.float64)
        sigma_array = np.empty(n, dtype=np.float64)
        oi_array = np.empty(n, dtype=np.float64)
        option_type_int_array = np.empty(n, dtype=np.int64)  # 0 для Call, 1 для Put
        
        # Заполняем массивы данными из опционов
        for i, option in enumerate(options_data):
            F0_array[i] = float(option.get('UNDERLYINGSETTLEPRICE', 0) or 0)
            K_array[i] = float(option.get('STRIKE', 0) or 0)
            
            # Рассчитываем время до экспирации
            expiry_date = option.get('LASTTRADEDATE')
            if expiry_date:
                T_minutes_array[i] = float(expiry_time(expiry_date) or 0)
            else:
                T_minutes_array[i] = 0.0
            
            r_array[i] = float(getattr(settings, 'IV_RATE', 0.08))
            sigma_array[i] = float(hist_vol_value or 0)
            
            option_type = str(option.get('OPTIONTYPE', 'C')).upper()
            option_type_int_array[i] = 0 if option_type == 'C' else 1
            
            oi_raw = option.get('PREVOPENPOSITION', 0)
            try:
                oi_array[i] = float(oi_raw) if oi_raw not in (None, "", "None", 0) else 0.0
            except (ValueError, TypeError):
                oi_array[i] = 0.0
        
        # Преобразуем минуты в годы
        trading_days_per_year = float(getattr(settings, 'TRADING_DAYS_PER_YEAR', 252))
        minutes_per_day = float(getattr(settings, 'MINUTES_PER_DAY', 865))
        T_years_array = T_minutes_array / (trading_days_per_year * minutes_per_day)
        
        # Маска валидных значений
        valid_mask = (
            (T_years_array > 1e-10) & 
            (sigma_array > 1e-10) & 
            (F0_array > 1e-10) & 
            (K_array > 1e-10) &
            (np.isfinite(T_years_array)) &
            (np.isfinite(sigma_array)) &
            (np.isfinite(F0_array)) &
            (np.isfinite(K_array))
        )
        
        # Применяем маску
        F0_valid = F0_array[valid_mask]
        K_valid = K_array[valid_mask]
        T_years_valid = T_years_array[valid_mask]
        r_valid = r_array[valid_mask]
        sigma_valid = sigma_array[valid_mask]
        option_type_int_valid = option_type_int_array[valid_mask]
        oi_valid = oi_array[valid_mask]
        
        # Инициализируем массивы результатов
        theoretical_price_array = np.zeros(n, dtype=np.float64)
        delta_array = np.zeros(n, dtype=np.float64)
        gamma_array = np.zeros(n, dtype=np.float64)
        vega_array = np.zeros(n, dtype=np.float64)
        theta_array = np.zeros(n, dtype=np.float64)
        gex_array = np.zeros(n, dtype=np.float64)
        
        # Рассчитываем только для валидных опционов с Numba
        if len(F0_valid) > 0:
            # ПРЕДРАСЧЕТ d1 и d2 для всех валидных опционов
            d1_valid, d2_valid = calculate_d1_d2_numba(F0_valid, K_valid, T_years_valid, sigma_valid)
            
            # Получаем настройки
            contract_multiplier = float(getattr(settings, "CONTRACT_MULTIPLIER", 1.0))
            gex_decimals = int(getattr(settings, "GEX_DECIMALS", 6))
            
            # Расчет всех параметров с использованием предрасчитанных d1, d2
            price_valid = calculate_black76_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid, 
                                                option_type_int_valid, d1_valid, d2_valid)
            delta_valid = calculate_delta_numba(F0_valid, T_years_valid, r_valid, option_type_int_valid, d1_valid)
            gamma_valid = calculate_gamma_numba(F0_valid, T_years_valid, r_valid, sigma_valid, d1_valid)
            vega_valid = calculate_vega_numba(F0_valid, T_years_valid, r_valid, d1_valid)
            theta_valid = calculate_theta_numba(F0_valid, K_valid, T_years_valid, r_valid, sigma_valid, 
                                              option_type_int_valid, d1_valid, d2_valid, trading_days_per_year)
            gex_valid = calculate_gex_numba(F0_valid, T_years_valid, gamma_valid, option_type_int_valid, oi_valid, contract_multiplier)
            
            # Заполняем результаты
            theoretical_price_array[valid_mask] = price_valid
            delta_array[valid_mask] = delta_valid
            gamma_array[valid_mask] = gamma_valid
            vega_array[valid_mask] = vega_valid
            theta_array[valid_mask] = theta_valid
            gex_array[valid_mask] = gex_valid
        
        # Обновляем опционы с результатами
        for i, option in enumerate(options_data):
            option['HIST_VOL'] = round(float(hist_vol_value), 4)
            option['IMPLIED_VOL'] = round(float(hist_vol_value), 4)
            option['THEORETICAL_PRICE'] = round(float(theoretical_price_array[i]), 2) if abs(theoretical_price_array[i]) > 1e-10 else 0.0
            option['DELTA'] = round(float(delta_array[i]), 6) if abs(delta_array[i]) > 1e-10 else 0.0
            option['GAMMA'] = round(float(gamma_array[i]), 6) if abs(gamma_array[i]) > 1e-10 else 0.0
            option['VEGA'] = round(float(vega_array[i]), 6) if abs(vega_array[i]) > 1e-10 else 0.0
            option['THETA'] = round(float(theta_array[i]), 6) if abs(theta_array[i]) > 1e-10 else 0.0
            option['GEX'] = round(float(gex_array[i]), gex_decimals) if abs(gex_array[i]) > 1e-10 else 0.0
            
    except Exception as e:
        # Логируем ошибку и возвращаем исходные данные
        print(f"Error in calculate_all_options_params_numba: {e}")
        for option in options_data:
            option['HIST_VOL'] = round(float(hist_vol_value), 4)
            option['IMPLIED_VOL'] = round(float(hist_vol_value), 4)
            option['THEORETICAL_PRICE'] = 0.0
            option['DELTA'] = 0.0
            option['GAMMA'] = 0.0
            option['VEGA'] = 0.0
            option['THETA'] = 0.0
            option['GEX'] = 0.0
    
    return options_data

# ---------------- АСИНХРОННАЯ ОБРАБОТКА ----------------

async def process_asset_options(options_data: List[dict], hist_vol_value: float) -> List[dict]:
    """
    Асинхронно обрабатывает все опционы актива с Numba-ускорением
    """
    try:
        # Запускаем в отдельном потоке чтобы не блокировать event loop
        loop = asyncio.get_event_loop()
        processed_data = await loop.run_in_executor(
            None, 
            calculate_all_options_params_numba, 
            options_data, 
            hist_vol_value
        )
        return processed_data
    except Exception as e:
        print(f"Error in process_asset_options: {e}")
        return options_data