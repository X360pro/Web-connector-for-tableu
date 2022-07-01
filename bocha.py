import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import pytz

try:
    connection = psycopg2.connect(user="postgres",
                                  password="1234",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="template1")
    cursor = connection.cursor()
    cursor.execute("select * from calcium_calendar_roster;")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records2 table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")