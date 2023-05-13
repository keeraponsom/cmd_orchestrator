import asyncio
import nest_asyncio
from pyzeebe import ZeebeTaskRouter, ZeebeWorker, create_insecure_channel, Job
import requests
import json

# Setup Channel and worker
channel = create_insecure_channel(hostname="localhost", port=26500)
worker = ZeebeWorker(channel)
# config function running method
nest_asyncio.apply()
# Save login session
session = requests.session()
login_url = "http://localhost:8081/api/login?username=demo&password=demo"
login_response = session.post(login_url)
print(f"Login response status code: {login_response.status_code}")

## Worker that doing service task.This is the example of performing complete usertask
@worker.task(task_type="io.camunda.zeebe:userTask", timeout_ms=100000)
async def my_task1(job1: Job):
    print("Hello")

asyncio.run(worker.work())