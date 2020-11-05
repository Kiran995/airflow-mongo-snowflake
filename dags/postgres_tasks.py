import pandas
import psycopg2
from sqlalchemy import create_engine

def connect_postgres():
    # Open a DB session
    dbSession = psycopg2.connect("dbname='postgres_db' user='sysadmin' password='mypass'");

    # Open a database cursor
    dbCursor = dbSession.cursor();
    return dbSession, dbCursor
# def connect_postgres():
#     connection = psycopg2.connect(user="sysadmin",
#                                   password="mypass",
#                                   host="127.0.0.1",
#                                   port="5432",
#                                   database="postgres_db")
#
#     cursor = connection.cursor()

def connect_sqlalchemy():
    engine = create_engine('postgresql://sysadmin:mypass@localhost:5432/postgres_db')
    return engine

if __name__ == '__main__':
    connect_postgres()
    connect_sqlalchemy()
