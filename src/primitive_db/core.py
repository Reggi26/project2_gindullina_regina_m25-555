#!/usr/bin/env python3

from typing import List, Tuple
from .utils import load_metadata, save_metadata

DB_META_FILE = 'db_meta.json'

SUPPORTED_TYPES = {'int', 'str', 'bool'}


def validate_column_definition(column_def: str) -> bool:
    if ':' not in column_def:
        return False
    
    name, col_type = column_def.split(':', 1)
    
    if not name.strip() or col_type not in SUPPORTED_TYPES:
        return False
    
    return True


def parse_column_definition(column_def: str) -> Tuple[str, str]:
    name, col_type = column_def.split(':', 1)
    return name.strip(), col_type


def get_metadata() -> dict:
    return load_metadata(DB_META_FILE)


def update_metadata(metadata: dict) -> None:
    save_metadata(DB_META_FILE, metadata)


def create_table(table_name: str, columns_defs: List[str]) -> Tuple[bool, str]:
    metadata = get_metadata()
    
    if table_name in metadata:
        return False, f'Таблица "{table_name}" уже существует.'
    
    validated_columns = ["ID:int"]
    
    for col_def in columns_defs:
        if not validate_column_definition(col_def):
            return False, f'Некорректное определение столбца: "{col_def}"'
        
        name, col_type = parse_column_definition(col_def)
        validated_columns.append(f"{name}:{col_type}")
    
    metadata[table_name] = {"columns": validated_columns}
    update_metadata(metadata)
    
    columns_str = ", ".join(validated_columns)
    return True, f'Таблица "{table_name}" успешно создана со столбцами: {columns_str}'


def drop_table(table_name: str) -> Tuple[bool, str]:
    metadata = get_metadata()
    
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    del metadata[table_name]
    update_metadata(metadata)
    
    return True, f'Таблица "{table_name}" успешно удалена.'


def list_tables() -> List[str]:
    metadata = get_metadata()
    return list(metadata.keys())