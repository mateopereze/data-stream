import os
import logging
from connection import DatabaseConnection

# Configuración del logger
logging.basicConfig(
    filename="etl_orchestration.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class ETL:
    def __init__(self, db_connection: DatabaseConnection, sql_folder: str):
        self.db_connection = db_connection
        self.sql_folder = sql_folder

    def execute_sql_script(self, script_path: str):
        """Ejecuta un archivo SQL desde la carpeta"""
        try:
            with open(script_path, "r") as file:
                sql_script = file.read()

            cursor = self.db_connection.connection.cursor()
            cursor.execute(sql_script)
            self.db_connection.connection.commit()
            logging.info(f"Script ejecutado: {script_path}")
            print(f"Script ejecutado: {script_path}")
        except Exception as e:
            logging.error(f"Error al ejecutar el script {script_path}: {e}")
            print(f"Error al ejecutar el script {script_path}: {e}")

    def execute_all_sql(self):
        """Ejecuta todos los archivos SQL de la carpeta en orden"""
        sql_files = sorted([f for f in os.listdir(self.sql_folder) if f.endswith(".sql")])
        for sql_file in sql_files:
            script_path = os.path.join(self.sql_folder, sql_file)
            self.execute_sql_script(script_path)
