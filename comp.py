import requests

url = 'http://localhost:8000/tasklist/complete'

payload = {
    'elementInstanceKey': 2251799813687832,
    "variables": {
        "firstName": "sss",
        "lastName": "jumpol.keawsehawong@gmail.com",
        "Age": 26,
        "email-id": "ss@gg",
        "password": "asdasd",
        "optionsEnumRadio":"Male"
        }
}

response = requests.post(url, json=payload)

if response.status_code == 200:
    print('Success:', response.content)
else:
    print('Error:', response.status_code)
