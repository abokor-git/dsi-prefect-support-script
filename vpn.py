from ping3 import ping
from prefect import task, flow
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment
import os
import subprocess

@task
def launch_vpn():
    server = os.getenv('SERVER')
    server_cert = os.getenv('SERVER_CERT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')

    # Construire la commande à exécuter
    command = f"openconnect {server} --servercert pin-sha256:{server_cert} --user={user} --key-password={password} <<EOF\n{password}\nEOF &"

    # Exécuter la commande en arrière-plan
    subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL)

@task
def other_task():
    ip = "10.39.234.26"
    result = ping(ip)
    print("résultat du ping prod",result)

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
        return Completed()
    
    return Failed()

####################################################################################

@flow
def my_flow():

    vpn_status = check_ip_availability.submit()
    result = vpn_status.result(raise_on_failure=False)
    if vpn_status.get_state().is_failed():
        x = launch_vpn.submit()
        y = other_task.submit(wait_for=[x])
        result = y.result(raise_on_failure=False)
        return Completed()
    else:
        y = other_task.submit(wait_for=[vpn_status])
        result = y.result(raise_on_failure=False)
        return Completed()

if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="vpn",
        flow=my_flow,
        work_queue_name="agent-prod",
        work_pool_name="xana-pool"
    )
    deployment.apply()

    my_flow()


