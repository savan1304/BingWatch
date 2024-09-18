import os
from sqlalchemy import create_engine, text
# from user_handler import User, EmailTracker, Base
from models.user_model import User, Base


#Pick DB credentials from environment variables
# host = os.getenv("TEST_DB_HOST")
host = "localhost"
# port = os.getenv("DB_PORT")
port = 5432
# database = os.getenv("POSTGRES_DB")
# user = os.getenv("POSTGRES_USER")
# password = os.getenv("POSTGRES_PASSWORD")

database = "airflow"
user ="airflow"
password = "airflow"

# Create the database URL
# db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
db_url = f"postgresql://airflow:airflow@postgres:{port}/airflow"

print("db_url in db.py",db_url)

#Function to connect to the database
def db_connect():
    try:
        engine = create_engine(db_url)
        connection = engine.connect()
        # connection.close()
        return connection
    except Exception as e:
        print(e)
        return False
    
#Function to create the engine
def db_engine():
    try:
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(e)
        return False
    
#Function to close the connection
def db_close(connection):
    try:
        connection.close()
        return True
    except Exception as e:
        return False
    
#Function to execute a query
def db_execute(connection, query):
    try:
        result = connection.execute(text(query))
        return result
    except Exception as e:
        return False
    
#Botstrapping Function 
def db_bootstrap():
    try:
        engine = db_engine()
        Base.metadata.create_all(engine)
        return True
    except Exception as e:
        print(e)
        return False



#################################################################################################################################################



# Testing Functions 
# print(db_bootstrap())
    