import json
import logging
import grpc
from zeebe_grpc import gateway_pb2, gateway_pb2_grpc

with grpc.insecure_channel("localhost:26500") as channel:
    stub = gateway_pb2_grpc.GatewayStub(channel)

    # start a worker
    activate_jobs_response = stub.ActivateJobs(
        gateway_pb2.ActivateJobsRequest(
            type="io.camunda.zeebe:userTask",
            worker="Python worker",
            timeout=60000,
            maxJobsToActivate=32
        )
    )
    for response in activate_jobs_response:
        for job in response.jobs:
            # if job.elementInstanceKey == 2251799813687130:
                try:
                    stub.CompleteJob(gateway_pb2.CompleteJobRequest(jobKey=job.key, variables=json.dumps({})))
                    logging.info("Job Completed")
                    print(job.elementInstanceKey)
                except Exception as e:
                    stub.FailJob(gateway_pb2.FailJobRequest(jobKey=job.key))
                    logging.info(f"Job Failed {e}")