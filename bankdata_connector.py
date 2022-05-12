import requests
import json
import psycopg2

url = "https://consulting.frankstillone.com/version-test/api/1.1/obj/BankData?constraints=[{\"key\":\"Created Date\",\"constraint_type\":\"greater than\",\"value\":\"2020-07-03\"}]"

payload={}
headers = {
  'Authorization': 'Bearer 5935b2c46af30ac330d4430d8d61ed73'
}

response = requests.request("GET", url, headers=headers, data=payload)
data = response.json()

try:
    connection = psycopg2.connect(user="blackcoffer",
                                  password="4321",
                                  host="194.195.252.78",
                                  port="5432",
                                  database="tableu_database")
    cursor = connection.cursor()

    for i in range(len(data['response']['results'])) :
      postgres_insert_query = """ INSERT INTO bubble_frankstillone_bankdata (bank_account_option_bank_account, cost_centre_custom_cost_centre, date_date, description_text, value_number, created_Date, created_By, modified_Date, id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
      record_to_insert = (data['response']['results'][i]['bank_account_option_bank_account'],
                        data['response']['results'][i]['cost_centre_custom_cost_centre'] if ('cost_centre_custom_cost_centre' in data['response']['results'][i].keys()) else None,
                        data['response']['results'][i]['date_date'],
                        data['response']['results'][i]['description_text'],
                        data['response']['results'][i]['value_number'],
                        data['response']['results'][i]['Created Date'],
                        data['response']['results'][i]['Created By'],
                        data['response']['results'][i]['Modified Date'],
                        data['response']['results'][i]['_id'])
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