from ping3 import ping
from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment

@task
def launch_vpn():
    print('Active VPN !!!')

@task
def other_task():
    print('Other Task !!!')

@task
def check_ip_availability():

    # Liste des adresses IP à tester
    ip_list = [
        "10.39.234.26",
        "10.39.234.54",
        "10.39.234.121",
        "10.10.5.26",
        "10.10.15.164",
        "10.10.15.165",
        "10.10.15.216",
        "10.10.15.217"
        ]

    for ip in ip_list:
        hist = []
        result = ping(ip)
        if result is not None:
            hist.append(True)
            break
        else:
            hist.append(False)

    if True in hist:
        return Completed(message="Connected")
    
    return Failed(message="Not Connected")

####################################################################################

@flow
def my_flow(task_runner=SequentialTaskRunner()):

    vpn_status = check_ip_availability()
    if vpn_status.is_failed():
        x = launch_vpn()
    y = other_task()

if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="vpn",
        flow=my_flow,
        work_queue_name="agent-prod",
        work_pool_name="xana-pool"
    )
    deployment.apply()

    my_flow()


