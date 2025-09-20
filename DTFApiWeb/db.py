from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

server = "server"
database = "data_base"
username = "user_name"
password = "********"

connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(connection_string)

SessionLocal = sessionmaker(autocommit = False, autoflush = False, bind = engine)

try:

    with engine.connect() as connection:

        result = connection.execute(text("SELECT 1"))
        print("Conexión a SQL Server exitosa.")

except Exception as e:

    print(f"❌ Error al conectar a SQL Server: {e}")

def get_db():

    db = SessionLocal()
    try:
        yield db

    finally:

        db.close()