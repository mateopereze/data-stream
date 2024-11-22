import pyodbc
from sqlalchemy import create_engine

class DatabaseConnection:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None

    def connect(self):
        """Establecer conexión con la base de datos usando pyodbc"""
        try:
            self.connection = pyodbc.connect(
                f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.server};"
                f"DATABASE={self.database};UID={self.username};PWD={self.password}"
            )
            print(f"Conexión establecida con {self.server}.")
        except Exception as e:
            print(f"Error al conectar con la base de datos: {e}")
            raise

    def get_engine(self):
        """Obtenemos un motor de SQLAlchemy para cargar datos en la base de datos"""
        return create_engine(f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server")

    def close(self):
        """Cerrar la conexión con la base de datos"""
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")
