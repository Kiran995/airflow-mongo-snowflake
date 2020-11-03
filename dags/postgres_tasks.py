import pandas
import psycopg2

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

if __name__ == '__main__':
    connect_postgres()
