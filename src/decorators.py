# src/decorators.py

import time
from functools import wraps
from typing import Any, Callable


def handle_db_errors(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except KeyError as e:
            error_msg = f"Ошибка доступа: ключ {e} не найден"
            if func.__name__ == 'select':
                return False, error_msg, []
            return False, error_msg
        except ValueError as e:
            error_msg = f"Ошибка валидации: {e}"
            if func.__name__ == 'select':
                return False, error_msg, []
            return False, error_msg
        except FileNotFoundError as e:
            error_msg = f"Файл не найден: {e}"
            if func.__name__ == 'select':
                return False, error_msg, []
            return False, error_msg
        except Exception as e:
            error_msg = f"Неожиданная ошибка: {e}"
            if func.__name__ == 'select':
                return False, error_msg, []
            return False, error_msg
    return wrapper


def confirm_action(action_name: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            message = f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: '
            print(message, end="")
            response = input().strip().lower()
            
            if response == 'y':
                return func(*args, **kwargs)
            else:
                print("Операция отменена.")
                if func.__name__ in ['delete', 'drop_table']:
                    return False, "Операция отменена"
                return False, "Операция отменена"
        return wrapper
    return decorator


def log_time(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed = end_time - start_time
        
        print(f"Функция {func.__name__} выполнилась за {elapsed:.3f} секунд")
        return result
    return wrapper


def create_cacher() -> Callable:
    cache = {}
    
    def cache_result(key: Any, value_func: Callable) -> Any:
        if key in cache:
            print(f"Используется кэш для ключа: {key}")
            return cache[key]
        
        result = value_func()
        cache[key] = result
        print(f"Сохранено в кэш для ключа: {key}")
        return result
    
    return cache_result