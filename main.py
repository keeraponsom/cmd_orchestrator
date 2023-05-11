import requests
import json
import datetime
import base64
import xmltodict

def main_kub():
    url = 'http://localhost:9200/zeebe-record_job_*/_search'

    response = requests.post(url)
    size = response.json()["hits"]['total']['value']

    query = {
        "size": 1000
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


    url_form = "http://localhost:9200/tasklist-form-1.0.0_/_search"
    query = {
        "size": 1000
    }
    response_form = requests.post(url_form, json=query)
    response_form = response_form.json()['hits']['hits']

    #save all variable to use in list
    listbpmnProcessId = []
    processInstanceKey = []
    Current_Process_ID = []
    Current_Instance_States = []
    Current_type = []
    errorMessage = []
    version = []
    processDefinitionKey = []
    camunda_form = []
    for i in fulldata:
        listbpmnProcessId.append(i['value']['bpmnProcessId'])
        processInstanceKey.append(i['value']['processInstanceKey'])
        Current_Process_ID.append(i['value']['elementId'])
        Current_Instance_States.append(i['intent'])
        Current_type.append(i['value']['type'])
        errorMessage.append(i['value']['errorMessage'])
        version.append(i['value']["processDefinitionVersion"])
        processDefinitionKey.append(i['value']["processDefinitionKey"])
    #try to append curent jsonform to list
        try:
            x = i['value']['customHeaders']['io.camunda.zeebe:formKey']
            my_dict = {}
            # Split the string by the colon separator
            string_parts = x.split(":")
            # Assign the value of the last part to the key in the dictionary
            my_dict[string_parts[0] + ":" + string_parts[1]] = string_parts[2]
            current_form_id = my_dict["camunda-forms:bpmn"]
            all_lastform = []
            lastform = []
            for x in response_form:
                if current_form_id == x['_source']['bpmnId']:
                    components = json.loads(x["_source"]["schema"])["components"]
                    lastform.append(components)
            all_lastform.append(lastform[-1])
            # print(lastform[-1])
            lastform = []
            # print(all_lastform)
            for x in response_form:
                if current_form_id == x['_source']['bpmnId']:
                    for components in all_lastform:
        ### Start convert CAMUNDA format to React
                        # Initialize the new dictionary object
                        new_data = {
                            "title": "",
                            "description": "A simple form example.",
                            "type": "object",
                            "properties": {}
                        }

                        # Loop through each item in the original data
                        for item in components:
                            if "text" in item:
                                new_data["title"] = item["text"].lstrip("#")
                            elif "label" in item:
                                label = item["label"]
                                field_type = item["type"]
                                field_id = item["key"]
                                if field_type == "textfield":
                                    field = {
                                        "type": "string",
                                        "title": label
                                    }
                                elif field_type == "number":
                                    field = {
                                        "type": "integer"
                                    }
                                elif field_type == "select":
                                    values = item["values"]
                                    enum_list = [value["label"] for value in values]
                                    field = {
                                        "type": "string",
                                        "title": label,
                                        "enum": enum_list
                                    }
                                else:
                                    continue
                                new_data["properties"][field_id] = field
                        camunda_form.append(new_data)
        except:
            camunda_form.append("")


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
            "jsonform":camunda_form[i],
        }
        for i in range(len(processInstanceKey))
    ]

    url = 'http://localhost:9200/zeebe-record_process_*/_search'

    response = requests.post(url)

    size = response.json()['_shards']['total']
    query = {
        "size": 1000
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
    # pretty_json = json.dumps(data, indent=4)
    return data
# print(main_kub())


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

# json_output = json.dumps(data, indent=4) << for check data
@app.get("/")
async def get_data():
    data = main_kub()
    # print(data)
    return JSONResponse(content=data)

@app.get("/dashboard/")
async def dashboard_data():
    data = main_kub()
    data_dashboard = [
    {
        "bpmnProcessId": item["bpmnProcessId"],
        "Current_Instance_Status": item["Current_Instance_Status"],
        "Robot": "This is Robot kub",  ### <<<<< ???
        "Start_time": item["Start_time"],
        "processInstanceKey": item["processInstanceKey"]
    } 
    for item in data
]
    return JSONResponse(content=data_dashboard)

@app.get("/tasklist/")
async def dashboard_data():
    data = main_kub()
    data_dashboard = []
    i = 0
    for item in data:
        if item["jsonform"] != "" and item["Current_Instance_Status"] == "Active":
            data_dashboard.append({
                "keys": i,
                "Current_Process_ID": item["Current_Process_ID"],
                "bpmnProcessId": item["bpmnProcessId"],
                "Creation Time": item["Start_time"],
                "Start_time": item["Start_time"],
                "Assignee": "???",
                "Current_Instance_Status": item["Current_Instance_Status"],
                "Task Form": item["jsonform"]
            })
            i += 1
    return JSONResponse(content=data_dashboard)


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)