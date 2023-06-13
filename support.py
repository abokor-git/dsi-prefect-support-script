from ping3 import ping
from prefect import task, flow
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment
from prefect_dask.task_runners import DaskTaskRunner
import time


@task
def print_values(values):
    for value in values:
        time.sleep(0.5)
        print(value, end="\r")


@flow(task_runner=DaskTaskRunner())
def support():

    print_values.submit(["AAAA"] * 15)
    print_values.submit(["BBBB"] * 10)


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Support Script",
        flow=support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pool"
    )
    deployment.apply()

    support()
