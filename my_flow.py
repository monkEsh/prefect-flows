import os
import csv
import datetime
from prefect import task, Flow, Parameter
from prefect.run_configs import LocalRun
from prefect.executors import LocalDaskExecutor
from prefect.storage import Local
# from prefect.schedules.schedules import IntervalSchedule

@task(max_retries=5, retry_delay=datetime.timedelta(seconds=5))
def extract(path):
    with open(path, "r") as f:
        text = f.readline().strip()
    data = [ int(i) for i in text.split(",")]
    return data

@task
def transform(data):
    tdata = [ i + 1 for i in data]
    return tdata

@task
def load(tdata, path):
    with open(path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(tdata)
    return

def build_flow():
    with Flow("my_flow", storage=Local()) as flow:
        # path = Parameter(name="path", required=True)
        path = os.environ.get("GREETING")
        data = extract(path=path)
        tdata = transform(data=data)
        result = load(tdata=tdata, path=path)
        data_2 = extract(path="values.csv", upstream_tasks=[result])
    return flow

# schedule = IntervalSchedule(
#     start_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
#     interval=datetime.timedelta(seconds=5)
# )
flow = build_flow()

# flow.visualize()

# flow.run(parameters={
#     "path": "values.csv"
# })

# Configure the `GREETING` environment variable for this flow
flow.run_config = LocalRun(env={"path": "values.csv"})

# Use a `LocalDaskExecutor` to run this flow
# This will run tasks in a thread pool, allowing for parallel execution
flow.executor = LocalDaskExecutor()

# Register the flow under the "tutorial" project
flow.register(project_name="tutorial")