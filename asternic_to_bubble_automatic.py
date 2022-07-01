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

