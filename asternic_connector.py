import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import pytz

f = open("credentials.json")
credentials = json.load(f)

tzInfo = pytz.timezone('Australia/ACT')

yesterdaysDate = datetime.now(tz=tzInfo) - timedelta(days = 1)
yesterdaysDate = yesterdaysDate.strftime('%Y-%m-%d')
todaysDate = datetime.now(tz=tzInfo).strftime('%Y-%m-%d')
threeMonthsDate = datetime.now(tz=tzInfo) + relativedelta(months=+3)

urlPrefix = "https://pbx.tsp.com.au/stats/rest/index.php?entity=search&start="
urlmid = "&end="

url = urlPrefix + str(yesterdaysDate) + urlmid + threeMonthsDate.strftime('%Y-%m-%d')

payload =  {'Username': credentials['asternicCredentials']['user'],
          'Password': credentials['asternicCredentials']['password']}

response = requests.get(url,
            auth = HTTPBasicAuth('restUser', 'w6fEM49Iswn8!bJQKcnxl9M4T'))

data = response.json()

try:
    connection = psycopg2.connect(user="postgres",
                                  password="Q7MRvA$Ne4Kx^0",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableau_database")
    cursor = connection.cursor()

    for i in range(len(data['rows'])) :
      postgres_insert_query = """ INSERT INTO aternic_cc_stats (overflow, uniqueid, clid, queuereal, did, datetime, dateStart, dateEnd, event, agent, agent_name, queuename, queue_name, waitTime, talkTime, combinedWaitTime, totalDuration, recordingFilename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (data['rows'][i]['overflow'] if ('overflow' in data['rows'][i].keys()) else None,
                        data['rows'][i]['uniqueid'] if ('uniqueid' in data['rows'][i].keys()) else None,
                        data['rows'][i]['clid'] if ('clid' in data['rows'][i].keys()) else None,
                        data['rows'][i]['queuereal'] if ('queuereal' in data['rows'][i].keys()) else None,
                        data['rows'][i]['did'] if ('did' in data['rows'][i].keys()) else None,
                        data['rows'][i]['datetime'] if ('datetime' in data['rows'][i].keys()) else None,
                        data['rows'][i]['dateStart'] if ('dateStart' in data['rows'][i].keys()) else None,
                        data['rows'][i]['dateEnd'] if ('dateEnd' in data['rows'][i].keys()) else None,
                        data['rows'][i]['event'] if ('event' in data['rows'][i].keys()) else None,
                        data['rows'][i]['agent'] if ('agent' in data['rows'][i].keys()) else None,
                        data['rows'][i]['agent_name'] if ('agent_name' in data['rows'][i].keys()) else None,
                        data['rows'][i]['queuename'] if ('queuename' in data['rows'][i].keys()) else None,
                        data['rows'][i]['queue_name'] if ('queue_name' in data['rows'][i].keys()) else None,
                        data['rows'][i]['waitTime'] if ('waitTime' in data['rows'][i].keys()) else None,
                        data['rows'][i]['talkTime'] if ('talkTime' in data['rows'][i].keys()) else None,
                        data['rows'][i]['combinedWaitTime'] if ('combinedWaitTime' in data['rows'][i].keys()) else None,
                        data['rows'][i]['totalDuration'] if ('totalDuration' in data['rows'][i].keys()) else None,
                        data['rows'][i]['recordingFilename'] if ('recordingFilename' in data['rows'][i].keys()) else None)
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print(count, "Record inserted successfully into table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records2 table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
