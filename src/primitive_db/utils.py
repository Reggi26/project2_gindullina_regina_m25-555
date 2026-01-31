# src/primitive_db/utils.py

import json
import os

from .constants import DATA_DIR


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def get_table_filepath(table_name: str) -> str:
    ensure_data_dir()
    return os.path.join(DATA_DIR, f"{table_name}.json")


def load_metadata(filepath: str) -> dict:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_metadata(filepath: str, data: dict) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_table_data(table_name: str) -> list:
    filepath = get_table_filepath(table_name)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []


def save_table_data(table_name: str, data: list) -> None:
    filepath = get_table_filepath(table_name)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)