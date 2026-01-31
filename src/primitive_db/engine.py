# src/primitive_db/engine.py

import shlex
from typing import List, Tuple
from .core import create_table, drop_table, list_tables
from .utils import load_metadata, save_metadata, DB_META_FILE


def print_help():
    """Выводит справочную информацию о командах."""
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


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


def run():
    print("***Процесс работы с таблицей***")
    print_help()
    
    while True:
        try:
            metadata = load_metadata(DB_META_FILE)
            
            user_input = input(">>>Введите команду: ").strip()
            
            if not user_input:
                continue
            
            try:
                cmd_name, args = parse_command(user_input)
            except ValueError as e:
                print(f"Ошибка: {e}")
                continue
            
            if cmd_name == "exit":
                print("Выход из программы. До свидания!")
                break
                
            elif cmd_name == "help":
                print_help()
                
            elif cmd_name == "list_tables":
                tables = list(metadata.keys())
                if tables:
                    for table in tables:
                        print(f"- {table}")
                else:
                    print("В базе данных нет таблиц.")
                    
            elif cmd_name == "create_table":
                if len(args) < 2:
                    print("Ошибка: Недостаточно аргументов. Использование: create_table <имя> <столбец1:тип> ...")
                else:
                    table_name = args[0]
                    columns = args[1:]
                    success, message = create_table(table_name, columns)
                    print(message)
                    
                    if success:
                        metadata = load_metadata(DB_META_FILE)
                    
            elif cmd_name == "drop_table":
                if len(args) != 1:
                    print("Ошибка: Неверное количество аргументов. Использование: drop_table <имя_таблицы>")
                else:
                    table_name = args[0]
                    success, message = drop_table(table_name)
                    print(message)
                    
                    if success:
                        metadata = load_metadata(DB_META_FILE)
                    
            else:
                print(f"Функции '{cmd_name}' нет. Попробуйте снова.")
                
        except EOFError:
            print("\nОбнаружен конец файла. Выход...")
            break
        except KeyboardInterrupt:
            print("\nПрограмма прервана пользователем.")
            break
        except Exception as e:
            print(f"Произошла ошибка: {e}")