import requests
import json
import datetime
import base64
import xmltodict
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

def fetch_data_from_url(url, query=None):
    response = requests.post(url, json=query)
    return response.json()

def process_data(data):
    non_dis = [hit['_source']['value']['processInstanceKey'] for hit in data['hits']['hits']]
    distinct_processInstanceKey = list(set(non_dis))
    varlist, timemax, timemin = [], [], []

    for process_key in distinct_processInstanceKey:
        variables = {}
        timecheck = []

        for hit in data['hits']['hits']:
            if hit['_source']['value']['processInstanceKey'] == process_key:
                variables.update(hit['_source']['value']['variables'])
                timecheck.append(int(hit['_source']['timestamp']))

        varlist.append({"variables": variables})
        timemax.append(max(timecheck))
        timemin.append(min(timecheck))

    return varlist, timemax, timemin

def extract_values_from_data(fulldata):
    return {
        key: [hit['value'][key] if key in hit['value'] else None for hit in fulldata]
        for key in ['bpmnProcessId', 'processInstanceKey', 'elementId', 'intent', 'type', 'errorMessage', 'processDefinitionVersion', 'processDefinitionKey']
    }


def process_timestamps(timestamps):
    return [str(datetime.datetime.fromtimestamp(int(ts) / 1000)) for ts in timestamps]

def main_kub():
    url = 'http://localhost:9200/zeebe-record_job_*/_search'
    data = fetch_data_from_url(url, {"size": 1000})
    varlist, timemax, timemin = process_data(data)

    fulldata = [
        hit['_source']
        for hit in data['hits']['hits']
        for z in timemax
        if str(hit['_source']['timestamp']) == str(z)
    ]

    extracted_values = extract_values_from_data(fulldata)
    timemaxconvert = process_timestamps(timemax)
    timeminconvert = process_timestamps(timemin)

    final_data = [
        {
            "keys": i,
            "bpmnProcessId": extracted_values["bpmnProcessId"][i],
            "processInstanceKey": extracted_values["processInstanceKey"][i],
            "Current_Process_ID": extracted_values["elementId"][i],
            "Current_Instance_Status": extracted_values["intent"][i],
            "Current_Version": extracted_values["processDefinitionVersion"][i],
            "errorMessage": extracted_values["errorMessage"][i],
            "Current_type": extracted_values["type"][i],
            "Start_time": timeminconvert[i],
            "End_time": timemaxconvert[i],
            "variables": varlist[i]["variables"],
            "processDefinitionKey": extracted_values["processDefinitionKey"][i],
        }
        for i in range(len(extracted_values["processInstanceKey"]))
    ]

    url = 'http://localhost:9200/zeebe-record_process_*/_search'
    data = fetch_data_from_url(url, {"size": 1000})

    for item in final_data:
        if item['Current_Instance_Status'] =='CREATED':
            item['End_time'] = "--"
            item['Current_Instance_Status'] = 'Active'
        for hit in data['hits']['hits']:
            if item["processDefinitionKey"] == hit['_source']['value']['processDefinitionKey']:
                xhtml = hit['_source']['value']['resource']
                decoded_string = base64.b64decode(str(xhtml))
                xml_dict = xmltodict.parse(decoded_string)
                xml_string = xmltodict.unparse(xml_dict)
                item['bpmnxml'] = f'{str(xml_string)}'
    return final_data

app = FastAPI()
origins = [""]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=[""],
    allow_headers=["*"],
)

@app.get("/")
async def get_data():
    data = main_kub()
    print(data)
    return JSONResponse(content=data)

@app.get("/dashboard/")
async def dashboard_data():
    data = main_kub()
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

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)