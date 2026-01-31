# src/primitive_db/parser.py

import shlex
from typing import Any, Dict, List


def parse_where_clause(where_str: str) -> Dict[str, Any]:
    if not where_str:
        return {}
    
    if '=' not in where_str:
        raise ValueError("Условие WHERE должно содержать оператор '='")
    
    parts = where_str.split('=', 1)
    if len(parts) != 2:
        raise ValueError("Некорректный формат условия WHERE")
    
    field = parts[0].strip()
    value_str = parts[1].strip()
    
    try:
        value = int(value_str)
    except ValueError:
        value_lower = value_str.lower()
        if value_lower in ('true', 'false'):
            value = value_lower == 'true'
        else:
            if (value_str.startswith('"') and value_str.endswith('"')) or \
               (value_str.startswith("'") and value_str.endswith("'")):
                value = value_str[1:-1]
            else:
                value = value_str
    
    return {field: value}


def parse_set_clause(set_str: str) -> Dict[str, Any]:
    if not set_str:
        return {}
    
    result = {}
    
    parts = []
    current_part = []
    in_quotes = False
    quote_char = None
    
    for char in set_str:
        if char in ('"', "'") and (not in_quotes or char == quote_char):
            in_quotes = not in_quotes
            if in_quotes:
                quote_char = char
            else:
                quote_char = None
        elif char == ',' and not in_quotes:
            parts.append(''.join(current_part).strip())
            current_part = []
            continue
        
        current_part.append(char)
    
    if current_part:
        parts.append(''.join(current_part).strip())
    
    for part in parts:
        if '=' not in part:
            raise ValueError(f"Некорректная часть в SET: '{part}'")
        
        field, value_str = part.split('=', 1)
        field = field.strip()
        value_str = value_str.strip()
        
        try:
            value = int(value_str)
        except ValueError:
            value_lower = value_str.lower()
            if value_lower in ('true', 'false'):
                value = value_lower == 'true'
            else:
                if (value_str.startswith('"') and value_str.endswith('"')) or \
                   (value_str.startswith("'") and value_str.endswith("'")):
                    value = value_str[1:-1]
                else:
                    value = value_str
        
        result[field] = value
    
    return result


def parse_insert_values(values_str: str) -> List[str]:
    try:
        return shlex.split(values_str)
    except ValueError:
        values = []
        current = []
        in_quotes = False
        quote_char = None
        
        for char in values_str:
            if char in ('"', "'") and (not in_quotes or char == quote_char):
                in_quotes = not in_quotes
                if in_quotes:
                    quote_char = char
                else:
                    quote_char = None
            elif char.isspace() and not in_quotes:
                if current:
                    values.append(''.join(current).strip())
                    current = []
                continue
            
            current.append(char)
        
        if current:
            values.append(''.join(current).strip())
        
        return values