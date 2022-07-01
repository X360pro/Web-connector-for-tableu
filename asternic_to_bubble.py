import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import pytz


url = "https://pbx.tsp.com.au/stats/rest/index.php?entity=search&start=2021-07-01&end=2022-05-30"

response = requests.get(url,
            auth = HTTPBasicAuth('restUser', 'w6fEM49Iswn8!bJQKcnxl9M4T'))

data = response.json()

headers = {
  'Authorization': 'Bearer cea2d5fb6e075c4ab8916e7841d3c57a'  
}

for i in range(len(data['rows'])):
    obj = {
        'agent_text' : data['rows'][i]['agent'],
        'agent_name_text' : data['rows'][i]['agent_name'],
        'datestart_date' : data['rows'][i]['dateStart'],
        'talktime_number' : data['rows'][i]['talkTime'],
        'clid_text' : data['rows'][i]['clid'],
        'combinedwaittime_number' : data['rows'][i]['combinedWaitTime'],
        'dateend_date' : data['rows'][i]['dateEnd'],
        'datetime_date' : data['rows'][i]['datetime'],
        'did_text' : data['rows'][i]['did'],
        'event_text' : data['rows'][i]['event'],
        'overflow_text' : data['rows'][i]['overflow'],
        'queue_name_text' : data['rows'][i]['queue_name'],
        'queuename_text' : data['rows'][i]['queuename'],
        'queuereal_text' : data['rows'][i]['queuereal'],
        'recordingfilename_text' : data['rows'][i]['recordingFilename'],
        'totalduration_number' : data['rows'][i]['totalDuration'],
        'uniqueid_text' : data['rows'][i]['uniqueid'],
        'waittime_number' : data['rows'][i]['waitTime']
      }
 
    response = requests.post('https://my.silentpartner.com.au/version-test/api/1.1/obj/asternic-cdr', headers=headers, data = obj)
    print(response.text)
