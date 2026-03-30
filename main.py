import os
import mysql.connector
from dotenv import load_dotenv
from pathlib import Path
from data_entry.data_generator import populate_database

def execute_sql_file(cursor, file_path):
    """
    Reads and executes statements from an SQL file one by one.
    Avoids 'multi=True' argument to ensure universal driver compatibility.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            
        # Split the script by the standard SQL delimiter
        sql_statements = sql_script.split(';')
            
        for statement in sql_statements:
            clean_statement = statement.strip()
            # Execute only if the statement is not empty
            if clean_statement:
                cursor.execute(clean_statement)
                
    except Exception as e:
        print(f"Error executing file {file_path}: {e}")
        raise

def main():
    load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not db_host or not db_user:
        print("Error: Credentials not found in .env file")
        return

    base_dir = Path(__file__).parent
    oltp_script = base_dir / "sql" / "transactional_schema.sql"
    dw_script = base_dir / "sql" / "data_warehouse_schema.sql"

    try:
        print("Connecting to MySQL server...")
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password
        )
        cursor = connection.cursor()

        print("--- [1/3] Executing OLTP creation script (transactional_schema.sql) ---")
        execute_sql_file(cursor, oltp_script)
        connection.commit()

        # Phase 2: Python Data Generation
        populate_database(connection)

        print("--- [3/3] Executing DW creation and ETL script (data_warehouse_schema.sql) ---")
        execute_sql_file(cursor, dw_script)
        connection.commit()

        print("\nPipeline executed successfully! The Data Warehouse is ready for PowerBI.")

    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()