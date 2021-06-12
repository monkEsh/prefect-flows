import prefect
from prefect import task, Flow
import datetime
import random
from time import sleep
from prefect.triggers import manual_only
from prefect.engine.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun

@task
def inc(x):
    sleep(random.random() / 10)
    return x + 1

@task
def dec(x):
    sleep(random.random() / 10)
    x =- 1
    return x

@task
def add(x, y):
    sleep(random.random() / 10)
    return x + y

@task
def list_sum(arr):
    logger = prefect.context.get("logger")  
    logger.info(f"total sum : {sum(arr)}")  
    return sum(arr)

with Flow("getting-started-example",
        executor=LocalDaskExecutor(), 
        run_config=LocalRun()) as flow:
    incs = inc.map(x=range(10))
    decs = dec.map(x=range(10))
    adds = add.map(incs, decs)
    total = list_sum(adds)
  
# flow.visualize()
flow.register(project_name = "Gitesh-test")
# flow.run()