import json
import requests

app_name = 'my.silentpartner.com.au'
app_version = 'version-test'
type_name = 'asternic-cdr'

with open('export_All-asternic-cdrs-modified--_2022-06-01_11-53-08.ndjson', 'r') as jsonFile:
    id_list = [json.loads(line) for line in jsonFile]


for i in range(len(id_list)):
    url = 'https://' + app_name + '/' + app_version + '/api/1.1/obj/' + type_name + '/' + id_list[i]["unique id"]
    response = requests.delete(url)
    print(response, 'for uniqueid : ', id_list[i]["unique id"])