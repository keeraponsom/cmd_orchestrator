import requests
import json
import datetime
import base64
import xmltodict
import schedule
import time

def update_data():
    global data
    data = main_kub()

def main_kub():
    url = 'http://localhost:9200/zeebe-record_job_*/_search'

    response = requests.post(url)
    size = response.json()["hits"]['total']['value']

    query = {
        "size": size
    }
    response = requests.post(url, json=query)
    non_dis = []
    fulldata = []
    timecheck = []
    timemax = []
    timemin = []

    ## Query distinct Process instant as distinct_list
    for i in response.json()['hits']['hits']:
        non_dis.append(i['_source']['value']['processInstanceKey'])
    distinct_processInstanceKey = list(set(non_dis))

    dict_z = {}
    dict_z['variables'] = {}
    varlist = []
    #Loop to get latest data for each process instant
    for z in distinct_processInstanceKey:
        for i in response.json()['hits']['hits']:
            if json.dumps(i['_source']['value']['processInstanceKey']) == str(z):
                dict_z["variables"].update(i['_source']['value']['variables']) # Do variables variable
                timecheck.append(int(json.dumps(i['_source']['timestamp'])))
        varlist.append(dict_z) # this is variable process
        dict_z = {}
        dict_z['variables'] = {} # clear dict variable >> Already get varlist
        timemax.append(max(timecheck))
        timemin.append(min(timecheck))
        timecheck = []

    ## already get data for use
    for z in timemax:
        for i in response.json()['hits']['hits']:
            if json.dumps(i['_source']['timestamp']) == str(z):
                fulldata.append(i['_source'])


    #save all variable to use in list
    listbpmnProcessId = []
    processInstanceKey = []
    Current_Process_ID = []
    Current_Instance_States = []
    Current_type = []
    errorMessage = []
    version = []
    processDefinitionKey = []
    for i in fulldata:
        listbpmnProcessId.append(i['value']['bpmnProcessId'])
        processInstanceKey.append(i['value']['processInstanceKey'])
        Current_Process_ID.append(i['value']['elementId'])
        Current_Instance_States.append(i['intent'])
        Current_type.append(i['value']['type'])
        errorMessage.append(i['value']['errorMessage'])
        version.append(i['value']["processDefinitionVersion"])
        processDefinitionKey.append(i['value']["processDefinitionKey"])

    timemaxconvert = []
    timeminconvert = []
    for i in timemax:
        timestamp = int(i)/1000
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        timemaxconvert.append(str(dt_object))


    for i in timemin:
        timestamp = int(i)/1000
        dt_object = datetime.datetime.fromtimestamp(timestamp)
        timeminconvert.append(str(dt_object))

    data = [
        {
            "keys":i,
            "bpmnProcessId": listbpmnProcessId[i],
            "processInstanceKey": processInstanceKey[i],
            "Current_Process_ID": Current_Process_ID[i],
            "Current_Instance_Status": Current_Instance_States[i],
            "Current_Version":version[i],
            "errorMessage":errorMessage[i],
            "Current_type": Current_type[i],
            "Start_time": timeminconvert[i],
            "End_time": timemaxconvert[i],
            "variables": varlist[i]["variables"],
            "processDefinitionKey":processDefinitionKey[i],
        }
        for i in range(len(listbpmnProcessId))
    ]

    url = 'http://localhost:9200/zeebe-record_process_*/_search'

    response = requests.post(url)

    size = response.json()['_shards']['total']
    query = {
        "size": size
    }
    response = requests.post(url, json=query)
    
    for i in data:
        if i['Current_Instance_Status'] == 'CREATED':
            i['End_time'] = "--"
            i['Current_Instance_Status'] = 'Active'
        for z in response.json()['hits']['hits']:
            if i["processDefinitionKey"] == z['_source']['value']['processDefinitionKey']:
                xhtml = z['_source']['value']['resource']
                decoded_string = base64.b64decode(str(xhtml))
                xml_dict = xmltodict.parse(decoded_string)
                xml_string = xmltodict.unparse(xml_dict)
                i['bpmnxml'] = f'{str(xml_string)}'
    return data

#run main_kub() for updating data
data = main_kub()
schedule.every(5).seconds.do(update_data)

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
origins = ["*"] # Change the * to the domain name of your frontend server.
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

json_output = json.dumps(data, indent=4)
print(json_output)
@app.get("/")
async def get_data():
    return JSONResponse(content=data)

@app.get("/dashboard/")
async def dashboard_data():
    data_dashboard = [
        {
            "bpmnProcessId": item["bpmnProcessId"],
            "Current_Instance_Status": item["Current_Instance_Status"],
            "Robot": "This is Robot kub",
            "Start_time": item["Start_time"],
            "processInstanceKey": item["processInstanceKey"]
        } 
        for item in data
    ]
    return JSONResponse(content=data_dashboard)

def run():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    import threading
    t = threading.Thread(target=run)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)

