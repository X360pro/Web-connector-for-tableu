import requests
import json
import psycopg2
from datetime import datetime,timedelta
import pytz

f = open("credentials.json")
credentials = json.load(f)

tzInfo = pytz.timezone('Australia/ACT')

urlPrefix = "https://consulting.frankstillone.com/api/1.1/obj/BankData?cursor="
urlCursorParam = "0"
urlEnd = "&limit=100&constraints=[{\"key\":\"Created Date\",\"constraint_type\":\"greater than\",\"value\":\""
urlDate = str(datetime.now(tz=tzInfo).strftime('%Y-%m-%d'))

url = urlPrefix + urlCursorParam + urlEnd + urlDate + "\"}]"

payload={}
headers = {
  'Authorization': 'Bearer ' + credentials['bubbleFrankstilloneAppCredentials']['Token']  
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()

def getRowsIntoPostgres(cursor,connection,data) :
    for i in range(len(data['response']['results'])) :
      postgres_insert_query = """ INSERT INTO bubble_frankstillone_bankdata (bank_account_option_bank_account, cost_centre_custom_cost_centre, date, description_text, value_number, created_Date, created_By, modified_Date, id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (data['response']['results'][i]['bank_account_option_bank_account'] if ('bank_account_option_bank_account' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['cost_centre_custom_cost_centre'] if ('cost_centre_custom_cost_centre' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['date_date'] if ('date_date' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['description_text'] if ('description_text' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['value_number'] if ('value_number' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['Created Date'],
                        data['response']['results'][i]['Created By'] if ('Created By' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['Modified Date'],
                        data['response']['results'][i]['_id'])
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print(count, "Record inserted successfully into mobile table")


try:
    connection = psycopg2.connect(user="postgres",
                                  password="Q7MRvA$Ne4Kx^0",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableau_database")
    cursor = connection.cursor()

    cursorParam = 0
    getRowsIntoPostgres(cursor,connection,data)
    cursorParam += 100

    while (data['response']['remaining'] > 0) :
        print(cursorParam,"rows inserted into database")
        urlCursorParam = str(cursorParam)

        url = urlPrefix + urlCursorParam + urlEnd + urlDate + "}]"

        payload={}
        headers = {
          'Authorization': 'Bearer ' + credentials['bubbleFrankstilloneAppCredentials']['Token']  
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()

        getRowsIntoPostgres(cursor,connection,data)

        cursorParam += 100
        

    

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records2 table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")