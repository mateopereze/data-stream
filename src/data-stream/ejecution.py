from connection import DatabaseConnection
from etl import ETL

class ETLExecution:
    def __init__(self, server, database, username, password, sql_folder):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.sql_folder = sql_folder

    def run(self):
        """Orquesta la ejecución de los pasos ETL"""
        # Paso 1: Establecer conexión
        db_connection = DatabaseConnection(self.server, self.database, self.username, self.password)
        db_connection.connect()

        # Paso 2: Crear un objeto ETL y ejecutar los scripts SQL
        etl = ETL(db_connection, self.sql_folder)
        etl.execute_all_sql()

        # Paso 3: Cerrar la conexión
        db_connection.close()

if __name__ == "__main__":
    # Configuración de la base de datos y carpeta SQL
    SERVER = "tu_servidor"
    DATABASE = "tu_base_de_datos"
    USERNAME = "tu_usuario"
    PASSWORD = "tu_contraseña"
    SQL_FOLDER = "sql"  # Carpeta con los scripts SQL

    # Ejecutar el proceso ETL
    etl_execution = ETLExecution(SERVER, DATABASE, USERNAME, PASSWORD, SQL_FOLDER)
    etl_execution.run()
