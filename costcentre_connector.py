import requests
import json
import psycopg2

f = open("credentials.json")
credentials = json.load(f)

tzInfo = pytz.timezone('Australia/ACT')

urlPrefix = "https://consulting.frankstillone.com/api/1.1/obj/CostCentre?cursor="
urlCursorParam = "0"
urlEnd = "&limit=100&constraints=[{\"key\":\"Created Date\",\"constraint_type\":\"greater than\",\"value\":\""
urlDate = str(datetime.now(tz=tzInfo).strftime('%Y-%m-%d'))

url = urlPrefix + urlCursorParam + urlEnd + urlDate + "\"}]"

headers = {
  'Authorization': 'Bearer ' + credentials['bubbleFrankstilloneAppCredentials']['Token']
}

response = requests.request("GET", url, headers=headers)
data = response.json()

def getRowsIntoPostgres(cursor,connection,data) :
  for i in range(len(data['response']['results'])) :
      postgres_insert_query = """ INSERT INTO bubble_frankstillone_costcentre (category_option_cost_centre_category, code_text, description_text, created_Date, created_By, modified_Date, id) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (data['response']['results'][i]['category_option_cost_centre_category'] if ('category_option_cost_centre_category' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['code_text'] if ('code_text' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['description_text'] if ('description_text' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['Created Date'] if ('Created Date' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['Created By'] if ('Created By' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['Modified Date'] if ('Modified Date' in data['response']['results'][i].keys()) else None,
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
    cursor.execute("DELETE FROM calcium_calendar_roster")
    connection.commit()

    cursorParam = 0
    getRowsIntoPostgres(cursor,connection,data)
    cursorParam += 100

    while (data['response']['remaining'] > 0) :
        urlCursorParam = str(cursorParam)

        url = urlPrefix + urlCursorParam + urlEnd

        payload={}
        headers = {
          'Authorization': 'Bearer ' + credentials['bubbleFrankstilloneAppCredentials']['Token']  
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()

        getRowsIntoPostgres(cursor,connection,data)

        cursorParam += 100
        print(cursorParam,"rows inserted into database")

    
except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into tableu_database and records table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
