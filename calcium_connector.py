import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
import pandas as pd
import io
import datetime

url = "https://calcium.silentpartner.com.au?CalendarName=Roster&User=restUser&Password=9g6MMqyJwtiM&Op=AdminExport&API=1&FromDate=2022-04-01&ToDate=2022-04-15&Format=msoutlook&save"

response = requests.get(url,
            auth = HTTPBasicAuth('restUser', '9g6MMqyJwtiM')).content

data = pd.read_csv(io.StringIO(response.decode('utf-8')))

try:
    connection = psycopg2.connect(user="blackcoffer",
                                  password="4321",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableu_database")
    cursor = connection.cursor()

    for index,rows in data.iterrows() :
      postgres_insert_query = """ INSERT INTO calcium_calendar_roster (subject, start_Date, start_Time, end_Date, end_Time, all_day_event, description, categories, owner_Username, calendar_Name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (rows['Subject'],
                          datetime.datetime.strptime(rows['Start Date'], '%d/%m/%Y').date(),
                          rows['Start Time'],
                          datetime.datetime.strptime(rows['End Date'], '%d/%m/%Y').date(),
                          rows['End Time'],
                          rows['All day event'],
                          rows['Description'],
                          rows['Categories'],
                          rows['Owner Username'],
                          rows['Calendar Name']
                        )
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print(count, "Record inserted successfully into  table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records in calcium_calendar_roster table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

