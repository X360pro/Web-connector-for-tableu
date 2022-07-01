import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
import pandas as pd
import io
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pytz
from calendar import monthrange

f = open("credentials.json")
credentials = json.load(f)

tzInfo = pytz.timezone('Australia/ACT')
todaysDate = datetime.datetime.now(tz=tzInfo).strftime('%Y-%m-%d')
threeMonthsDate = datetime.datetime.now(tz=tzInfo) + relativedelta(months=+3)

numOfDaysInMonth = monthrange(int(datetime.datetime.now(tz=tzInfo).strftime('%Y')), int(datetime.datetime.now(tz=tzInfo).strftime('%m')))[1]
threeMonthsDate = threeMonthsDate.replace(day=numOfDaysInMonth)

urlPrefix = "https://calcium.silentpartner.com.au?CalendarName=Roster&User=restUser&Password=9g6MMqyJwtiM&Op=AdminExport&API=1&FromDate=2000-01-01&ToDate="
urlEnd = "&Format=msoutlook&save"

url = urlPrefix + threeMonthsDate.strftime('%Y-%m-%d') + urlEnd

print(url)

response = requests.get(url,
            auth = HTTPBasicAuth(credentials['calciumCredentials']['user'], credentials['calciumCredentials']['password'])).content

data = pd.read_csv(io.StringIO(response.decode('utf-8')))

try:
    connection = psycopg2.connect(user="postgres",
                                  password="Q7MRvA$Ne4Kx^0",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableau_database")
    cursor = connection.cursor()
    cursor.execute("DELETE FROM calcium_calendar_roster")
    connection.commit()

    for index,rows in data.iterrows() :
      postgres_insert_query = """ INSERT INTO calcium_calendar_roster (subject, start_Date, start_Time, end_Date, end_Time, all_day_event, description, categories, owner_Username, calendar_Name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (rows['Subject'],
                          datetime.datetime.strptime(rows['Start Date'], '%d/%m/%Y').date(),
                          rows['Start Time'] if (not(pd.isnull(rows['Start Time']))) else None,
                          datetime.datetime.strptime(rows['End Date'], '%d/%m/%Y').date(),
                          rows['End Time'] if (not(pd.isnull(rows['End Time']))) else None,
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

