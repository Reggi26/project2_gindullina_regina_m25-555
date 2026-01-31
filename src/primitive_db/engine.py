# src/primitive_db/engine.py

import shlex
from typing import List, Tuple
from prettytable import PrettyTable
from .core import (
    create_table, drop_table, list_tables,
    insert, select, update, delete, delete_all,
    get_table_schema
)
from .parser import parse_where_clause, parse_set_clause, parse_insert_values
from .utils import load_metadata, DB_META_FILE


def print_help():
    print("\n" + "="*60)
    print("ПРИМИТИВНАЯ БАЗА ДАННЫХ - CRUD ОПЕРАЦИИ")
    print("="*60)
    
    print("\nУПРАВЛЕНИЕ ТАБЛИЦАМИ:")
    print("  create_table <имя> <столбец1:тип> .. - создать таблицу")
    print("  list_tables                          - список таблиц")
    print("  drop_table <имя>                     - удалить таблицу")
    
    print("\nCRUD ОПЕРАЦИИ:")
    print("  insert <таблица> <значение1> <значение2> ... - добавить запись")
    print("  select <таблица> [where условие]             - выбрать записи")
    print("  update <таблица> set ... [where условие]     - обновить записи")
    print("  delete <таблица> [where условие]             - удалить записи")
    print("  delete_all <таблица>                         - удалить ВСЕ записи")
    
    print("\nОБЩИЕ КОМАНДЫ:")
    print("  describe <таблица>                 - показать структуру таблицы")
    print("  exit                               - выход")
    print("  help                               - эта справка")
    
    print("\nПРИМЕРЫ:")
    print("  create_table users name:str age:int is_active:bool")
    print("  insert users 'John Doe' 25 true")
    print("  select users where age = 25")
    print("  update users set age = 30 where name = 'John Doe'")
    print("  delete users where name = 'John Doe'")
    print("="*60 + "\n")


def parse_command(command: str) -> Tuple[str, List[str]]:
    try:
        parts = shlex.split(command.strip())
        if not parts:
            return "", []
        
        cmd_name = parts[0]
        args = parts[1:] if len(parts) > 1 else []
        return cmd_name, args
    except ValueError as e:
        raise ValueError(f"Ошибка разбора команды: {e}")


def extract_where_clause(args: List[str]) -> Tuple[List[str], dict]:
    where_clause = {}
    if 'where' in args:
        where_index = args.index('where')
        where_str = ' '.join(args[where_index + 1:])
        args = args[:where_index]
        
        try:
            where_clause = parse_where_clause(where_str)
        except ValueError as e:
            raise ValueError(f"Ошибка в условии WHERE: {e}")
    
    return args, where_clause


def extract_set_clause(args: List[str]) -> Tuple[List[str], dict]:
    """Извлекает условие SET из аргументов."""
    set_clause = {}
    if 'set' in args:
        set_index = args.index('set')
        if 'where' in args[set_index:]:
            where_index = args.index('where', set_index)
            set_str = ' '.join(args[set_index + 1:where_index])
            args = args[:set_index] + args[where_index:]
        else:
            set_str = ' '.join(args[set_index + 1:])
            args = args[:set_index]
        
        try:
            set_clause = parse_set_clause(set_str)
        except ValueError as e:
            raise ValueError(f"Ошибка в условии SET: {e}")
    
    return args, set_clause


def format_table_result(data: List[dict]) -> str:
    if not data:
        return "Нет данных для отображения"
    
    table = PrettyTable()
    
    first_record = data[0]
    columns = ['ID'] + [col for col in first_record.keys() if col != 'ID']
    
    table.field_names = columns
    table.align = 'l'
    
    for record in data:
        row = [record.get(col, '') for col in columns]
        table.add_row(row)
    
    return str(table)


def run():
    print("="*60)
    print("ПРИМИТИВНАЯ БАЗА ДАННЫХ ЗАПУЩЕНА!")
    print("="*60)
    
    while True:
        try:
            user_input = input("\n>>> Введите команду: ").strip()
            
            if not user_input:
                continue
            
            try:
                cmd_name, args = parse_command(user_input)
            except ValueError as e:
                print(f" {e}")
                continue
            
            if cmd_name == "exit":
                print("Выход из программы. До свидания!")
                break
                
            elif cmd_name == "help":
                print_help()
                
            elif cmd_name == "list_tables":
                metadata = load_metadata(DB_META_FILE)
                tables = list(metadata.keys())
                if tables:
                    print("\nСписок таблиц:")
                    for table in tables:
                        print(f"  - {table}")
                else:
                    print("В базе данных нет таблиц.")
                    
            elif cmd_name == "create_table":
                if len(args) < 2:
                    print("Ошибка: Недостаточно аргументов.")
                    print("   Использование: create_table <имя> <столбец1:тип> ...")
                else:
                    table_name = args[0]
                    columns = args[1:]
                    success, message = create_table(table_name, columns)
                    print(f"{message}" if success else f"{message}")
                    
            elif cmd_name == "drop_table":
                if len(args) != 1:
                    print("Ошибка: Неверное количество аргументов.")
                    print("   Использование: drop_table <имя_таблицы>")
                else:
                    table_name = args[0]
                    success, message = drop_table(table_name)
                    print(f"{message}" if success else f"{message}")
                    
            elif cmd_name == "describe":
                if len(args) != 1:
                    print("Ошибка: Неверное количество аргументов.")
                    print("   Использование: describe <имя_таблицы>")
                else:
                    table_name = args[0]
                    try:
                        schema = get_table_schema(table_name)
                        print(f"\n📊 Структура таблицы '{table_name}':")
                        for name, col_type in schema:
                            print(f"  - {name}: {col_type}")
                    except ValueError as e:
                        print(f" {e}")
                        
            elif cmd_name == "insert":
                if len(args) < 2:
                    print(" Ошибка: Недостаточно аргументов.")
                    print("   Использование: insert <таблица> <значение1> <значение2> ...")
                else:
                    table_name = args[0]
                    values = parse_insert_values(' '.join(args[1:]))
                    success, message = insert(table_name, values)
                    print(f" {message}" if success else f" {message}")
                    
            elif cmd_name == "select":
                if len(args) < 1:
                    print(" Ошибка: Недостаточно аргументов.")
                    print("   Использование: select <таблица> [where условие]")
                else:
                    table_name = args[0]
                    try:
                        remaining_args, where_clause = extract_where_clause(args[1:])
                        success, message, data = select(table_name, where_clause)
                        
                        if success:
                            if data:
                                print(f"\n{message}:")
                                print(format_table_result(data))
                            else:
                                print(f"  {message}")
                        else:
                            print(f" {message}")
                    except ValueError as e:
                        print(f" {e}")
                        
            elif cmd_name == "update":
                if len(args) < 1:
                    print(" Ошибка: Недостаточно аргументов.")
                    print("   Использование: update <таблица> set ... [where условие]")
                else:
                    table_name = args[0]
                    try:
                        # Извлекаем SET и WHERE
                        remaining_args, set_clause = extract_set_clause(args[1:])
                        if not set_clause:
                            print(" Ошибка: Отсутствует условие SET")
                            print("   Использование: update <таблица> set поле=значение [where условие]")
                            continue
                        
                        remaining_args, where_clause = extract_where_clause(remaining_args)
                        
                        if not where_clause:
                            print("  Внимание: Будет обновлено ВСЕ записи в таблице!")
                            confirm = input("   Продолжить? (yes/no): ").strip().lower()
                            if confirm != 'yes':
                                print(" Операция отменена")
                                continue
                        
                        success, message = update(table_name, set_clause, where_clause)
                        print(f" {message}" if success else f" {message}")
                    except ValueError as e:
                        print(f" {e}")
                        
            elif cmd_name == "delete":
                if len(args) < 1:
                    print(" Ошибка: Недостаточно аргументов.")
                    print("   Использование: delete <таблица> [where условие]")
                else:
                    table_name = args[0]
                    try:
                        remaining_args, where_clause = extract_where_clause(args[1:])
                        
                        if not where_clause:
                            print("  Внимание: Для удаления всех записей используйте 'delete_all'")
                            continue
                        
                        success, message = delete(table_name, where_clause)
                        print(f" {message}" if success else f" {message}")
                    except ValueError as e:
                        print(f" {e}")
                        
            elif cmd_name == "delete_all":
                if len(args) != 1:
                    print(" Ошибка: Неверное количество аргументов.")
                    print("   Использование: delete_all <таблица>")
                else:
                    table_name = args[0]
                    print(f"  ВНИМАНИЕ: Вы собираетесь удалить ВСЕ записи из таблицы '{table_name}'!")
                    confirm = input("   Это действие нельзя отменить. Продолжить? (yes/no): ").strip().lower()
                    if confirm == 'yes':
                        success, message = delete_all(table_name)
                        print(f" {message}" if success else f" {message}")
                    else:
                        print(" Операция отменена")
                    
            else:
                print(f" Команда '{cmd_name}' не найдена. Введите 'help' для справки.")
                
        except EOFError:
            print("\n Обнаружен конец файла. Выход...")
            break
        except KeyboardInterrupt:
            print("\n  Программа прервана пользователем.")
            break
        except Exception as e:
            print(f" Неожиданная ошибка: {e}")