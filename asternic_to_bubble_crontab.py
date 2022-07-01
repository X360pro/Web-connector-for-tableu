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
previousDateTmeForBubble = datetime.now(tz=tzInfo) - timedelta(days = 1)
tomorrowsDateTimeForBubble = datetime.now(tz=tzInfo) + relativedelta(days =+1)

todaysDateForAsternic = datetime.now(tz=tzInfo).strftime('%Y-%m-%d')

urlForAsternic = "https://pbx.tsp.com.au/stats/rest/index.php?entity=search&start=" + todaysDateForAsternic + "&end=" + todaysDateForAsternic

prefixUrlForBubble = "https://my.silentpartner.com.au/version-test/api/1.1/obj/asternic-cdr?constraints=[{\"key\":\"datetime_date\",\"constraint_type\":\"greater than\",\"value\":\"" +  previousDateTmeForBubble.strftime('%Y-%m-%d %H:%M:%S') + "\"},{\"key\":\"datetime_date\",\"constraint_type\":\"less than\",\"value\":\"" + tomorrowsDateTimeForBubble.strftime('%Y-%m-%d %H:%M:%S') + "\"}," + "{\"key\":\"uniqueid_text\",\"constraint_type\":\"equals\",\"value\":\""

response = requests.get(urlForAsternic,
            auth = HTTPBasicAuth(credentials['asternicCredentials']['user'], credentials['asternicCredentials']['password']))
data = response.json()



for i in range(len(data['rows'])):
    urlForBubble = prefixUrlForBubble + str(data['rows'][i]['uniqueid']) + "\"}]"
    response = requests.get(urlForBubble, headers = {'Authorization': 'Bearer cea2d5fb6e075c4ab8916e7841d3c57a'})
    response = response.json()
    print(urlForBubble)
    if len(response["response"]["results"]) == 0 :
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
 
        response = requests.post('https://my.silentpartner.com.au/version-test/api/1.1/obj/asternic-cdr', headers = {'Authorization': 'Bearer cea2d5fb6e075c4ab8916e7841d3c57a'}, data = obj)
        print(response.text)

