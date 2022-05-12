import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2

url = "https://pbx.tsp.com.au/stats/rest/index.php?entity=search&start=2022-05-01&end=2022-05-01"

payload =  {'Username': 'restUser',
          'Password': 'w6fEM49Iswn8!bJQKcnxl9M4T'}

response = requests.get(url,
            auth = HTTPBasicAuth('restUser', 'w6fEM49Iswn8!bJQKcnxl9M4T'))

data = response.json()

try:
    connection = psycopg2.connect(user="blackcoffer",
                                  password="4321",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableu_database")
    cursor = connection.cursor()

    for i in range(len(data['rows'])) :
      postgres_insert_query = """ INSERT INTO aternic_cc_stats (overflow, uniqueid, clid, queuereal, did, datetime, dateStart, dateEnd, event, agent, agent_name, queuename, queue_name, waitTime, talkTime, combinedWaitTime, totalDuration, recordingFilename) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (data['rows'][i]['overflow'],
                        data['rows'][i]['uniqueid'],
                        data['rows'][i]['clid'],
                        data['rows'][i]['queuereal'],
                        data['rows'][i]['did'],
                        data['rows'][i]['datetime'],
                        data['rows'][i]['dateStart'],
                        data['rows'][i]['dateEnd'],
                        data['rows'][i]['event'],
                        data['rows'][i]['agent'],
                        data['rows'][i]['agent_name'],
                        data['rows'][i]['queuename'],
                        data['rows'][i]['queue_name'],
                        data['rows'][i]['waitTime'],
                        data['rows'][i]['talkTime'],
                        data['rows'][i]['combinedWaitTime'],
                        data['rows'][i]['totalDuration'],
                        data['rows'][i]['recordingFilename'])
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print(count, "Record inserted successfully into mobile table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records2 table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
