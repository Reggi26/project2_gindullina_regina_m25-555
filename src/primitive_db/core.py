# src/primitive_db/core.py

from typing import List, Tuple, Dict, Any, Optional
from .utils import load_metadata, save_metadata, load_table_data, save_table_data

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
    
    save_table_data(table_name, [])
    
    columns_str = ", ".join(validated_columns)
    return True, f'Таблица "{table_name}" успешно создана со столбцами: {columns_str}'


def drop_table(table_name: str) -> Tuple[bool, str]:
    metadata = get_metadata()
    
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    del metadata[table_name]
    update_metadata(metadata)
    
    import os
    from .utils import get_table_filepath
    filepath = get_table_filepath(table_name)
    if os.path.exists(filepath):
        os.remove(filepath)
    
    return True, f'Таблица "{table_name}" успешно удалена.'


def list_tables() -> List[str]:
    metadata = get_metadata()
    return list(metadata.keys())


def parse_value(value_str: str, expected_type: str) -> Any:
    """Преобразует строковое значение в нужный тип."""
    if expected_type == 'int':
        try:
            return int(value_str)
        except ValueError:
            raise ValueError(f"Невозможно преобразовать '{value_str}' в int")
    elif expected_type == 'bool':
        value_str = value_str.lower()
        if value_str in ('true', '1', 'yes'):
            return True
        elif value_str in ('false', '0', 'no'):
            return False
        else:
            raise ValueError(f"Невозможно преобразовать '{value_str}' в bool")
    elif expected_type == 'str':
        if (value_str.startswith('"') and value_str.endswith('"')) or \
           (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]
        return value_str
    else:
        raise ValueError(f"Неподдерживаемый тип: {expected_type}")


def get_table_schema(table_name: str) -> List[Tuple[str, str]]:
    """Возвращает схему таблицы в виде списка (имя, тип)."""
    metadata = get_metadata()
    if table_name not in metadata:
        raise ValueError(f"Таблица '{table_name}' не существует")
    
    columns = metadata[table_name]["columns"]
    schema = []
    for col_def in columns:
        if col_def == "ID:int":
            schema.append(("ID", "int"))
        else:
            name, col_type = parse_column_definition(col_def)
            schema.append((name, col_type))
    return schema


def insert(table_name: str, values: List[str]) -> Tuple[bool, str]:

    metadata = get_metadata()
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    table_data = load_table_data(table_name)
    
    schema = get_table_schema(table_name)
    user_columns = schema[1:]  # Пропускаем ID
    
    if len(values) != len(user_columns):
        return False, f'Ожидается {len(user_columns)} значений, получено {len(values)}'
    
    if table_data:
        max_id = max(record.get('ID', 0) for record in table_data)
        new_id = max_id + 1
    else:
        new_id = 1
    
    new_record = {"ID": new_id}
    
    for (col_name, col_type), value_str in zip(user_columns, values):
        try:
            parsed_value = parse_value(value_str, col_type)
            new_record[col_name] = parsed_value
        except ValueError as e:
            return False, f'Ошибка в столбце "{col_name}": {e}'
    
    table_data.append(new_record)
    save_table_data(table_name, table_data)
    
    return True, f'Запись успешно добавлена с ID={new_id}'


def select(table_name: str, where_clause: Optional[Dict[str, Any]] = None) -> Tuple[bool, str, List[Dict]]:

    metadata = get_metadata()
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.', []
    
    table_data = load_table_data(table_name)
    
    if not table_data:
        return True, "Таблица пуста", []
    
    if where_clause:
        filtered_data = []
        for record in table_data:
            match = True
            for key, expected_value in where_clause.items():
                if key not in record or record[key] != expected_value:
                    match = False
                    break
            if match:
                filtered_data.append(record)
        result_data = filtered_data
    else:
        result_data = table_data
    
    message = f'Найдено {len(result_data)} записей' if result_data else "Записи не найдены"
    return True, message, result_data


def update(table_name: str, set_clause: Dict[str, Any], where_clause: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:

    metadata = get_metadata()
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    table_data = load_table_data(table_name)
    
    schema = get_table_schema(table_name)
    schema_dict = dict(schema)
    
    for field in set_clause.keys():
        if field not in schema_dict:
            return False, f'Поле "{field}" не существует в таблице'
    
    updated_count = 0
    
    for record in table_data:
        match = True
        if where_clause:
            for key, expected_value in where_clause.items():
                if key not in record or record[key] != expected_value:
                    match = False
                    break
        
        if match:
            updated_count += 1
            for field, new_value in set_clause.items():
                if field in schema_dict:
                    expected_type = schema_dict[field]
                    try:
                        if isinstance(new_value, str):
                            parsed_value = parse_value(new_value, expected_type)
                        else:
                            parsed_value = new_value
                        record[field] = parsed_value
                    except ValueError as e:
                        return False, f'Ошибка в поле "{field}": {e}'
    
    if updated_count > 0:
        save_table_data(table_name, table_data)
        return True, f'Обновлено {updated_count} записей'
    else:
        return True, "Записи для обновления не найдены"


def delete(table_name: str, where_clause: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    metadata = get_metadata()
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    table_data = load_table_data(table_name)
    
    if not table_data:
        return True, "Таблица пуста"
    

    if not where_clause:
        return False, "Для удаления всех записей используйте команду 'delete_all'"
    
    records_to_keep = []
    deleted_count = 0
    
    for record in table_data:
        match = True
        for key, expected_value in where_clause.items():
            if key not in record or record[key] != expected_value:
                match = False
                break
        
        if match:
            deleted_count += 1
        else:
            records_to_keep.append(record)
    
    if deleted_count > 0:
        save_table_data(table_name, records_to_keep)
        return True, f'Удалено {deleted_count} записей'
    else:
        return True, "Записи для удаления не найдены"


def delete_all(table_name: str) -> Tuple[bool, str]:
    metadata = get_metadata()
    if table_name not in metadata:
        return False, f'Таблица "{table_name}" не существует.'
    
    save_table_data(table_name, [])
    return True, f'Все записи из таблицы "{table_name}" удалены'