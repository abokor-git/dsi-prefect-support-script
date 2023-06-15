from prefect import task, flow
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment
from prefect_dask.task_runners import DaskTaskRunner

import mysql.connector
import psycopg2
from psycopg2 import Error

import os
import subprocess

from ping3 import ping

import pandas as pd
import time


@task
def save_data(data):

    try:

        xana_host = os.getenv('XANA_HOST')
        xana_user = os.getenv('XANA_USER')
        xana_password = os.getenv('XANA_PASSWORD')
        xana_name = os.getenv('XANA_NAME')

        # Connexion à la base de données PostgreSQL
        connection = psycopg2.connect(
            host=xana_host,
            database=xana_name,
            user=xana_user,
            password=xana_password
        )

        cursor = connection.cursor()

        for index in data.index:

            row = data.loc[index]

            if row["response"] != '':

                print(row["id"])

                insert_query = "INSERT INTO public.home_supportresponses(response, supportrequest_id) VALUES (%s, %s)"
                record_to_insert = (str(row["response"]), int(row["id"]))
                cursor.execute(insert_query, record_to_insert)

                update_query = "UPDATE public.home_supportrequests SET is_processed = %s WHERE id = %s"
                record_to_update = ('true', int(row["id"]))
                cursor.execute(update_query, record_to_update)

        connection.commit()
        cursor.close()
        connection.close()

        return Completed()

    except (Exception, psycopg2.Error) as error:

        return Completed()


@task
def print_aaa_data(data):
    time.sleep(5)
    print(data)


@task
def print_bscs_data(data):
    time.sleep(8)
    print(data)


@task
def print_dpi_data(data):
    time.sleep(11)
    print(data)


@task
def print_elastic_data(data):
    time.sleep(14)
    print(data)


@task
def print_ocs_data(data):
    time.sleep(17)
    print(data)


@task
def get_topup_prod_data(data):

    response_json = []

    try:

        topup_host = os.getenv('TOPUP_HOST')
        topup_user = os.getenv('TOPUP_USER')
        topup_password = os.getenv('TOPUP_PASSWORD')
        topup_name = os.getenv('TOPUP_NAME')

        # Se connecter à la base de données MySQL
        connection = mysql.connector.connect(
            host=topup_host,
            user=topup_user,
            password=topup_password,
            database=topup_name
        )

        # Créer un curseur pour exécuter les requêtes
        cursor = connection.cursor()

        for x in data["payload"]:

            # Exécuter la requête SELECT
            queue_hist_query = "select id, action_type, command_content, created_date, status, updated_date, error_description, msisdn, pincode, error_code, etat, thread_name, thread_number, fee, ip_adresse_client, username, ext_transaction_id, skip from TOPUPDJIB.queue_hist where command_content like '%{}%' or msisdn like '%{}%' order by created_date desc limit 20;".format(
                x, x)
            cursor.execute(queue_hist_query)
            queue_hist_rows = cursor.fetchall()

            # Exécuter la requête SELECT
            queue_query = "select id, action_type, command_content, created_date, status, updated_date, error_description, msisdn, pincode, error_code, etat, thread_name, thread_number, fee, ip_adresse_client, username, ext_transaction_id, skip from TOPUPDJIB.queue where command_content like '%{}%' or msisdn like '%{}%' order by created_date desc limit 20;".format(
                x, x)
            cursor.execute(queue_query)
            queue_rows = cursor.fetchall()

            columns = ["id", "action_type", "command_content", "created_date", "status", "updated_date", "error_description", "msisdn", "pincode",
                       "error_code", "etat", "thread_name", "thread_number", "fee", "ip_adresse_client", "username", "ext_transaction_id", "skip"]

            df = pd.DataFrame(queue_hist_rows, columns=columns)
            queue_hist_json_data = df.to_json(orient='split')

            df = pd.DataFrame(queue_rows, columns=columns)
            queue_json_data = df.to_json(orient='split')

            resp_json = {
                'queue_hist': queue_hist_json_data,
                'queue': queue_json_data
            }

            response_json.append(resp_json)

        # Fermer le curseur et la connexion
        cursor.close()
        connection.close()

        data["response"] = response_json

    except mysql.connector.Error as error:
        # Gérer l'erreur de la base de données
        data["response"] = ''
        print(f"Erreur lors de l'exécution de la requête : {error}")


@task
def filtered_data(df, platform):

    df_filtered = df.loc[df['platform'] == platform]

    return df_filtered


@task
def get_support_request():

    try:

        xana_host = os.getenv('XANA_HOST')
        xana_user = os.getenv('XANA_USER')
        xana_password = os.getenv('XANA_PASSWORD')
        xana_name = os.getenv('XANA_NAME')

        # Connexion à la base de données PostgreSQL
        connection = psycopg2.connect(
            host=xana_host,
            database=xana_name,
            user=xana_user,
            password=xana_password
        )

        # Création d'un curseur pour exécuter les requêtes
        cursor = connection.cursor()

        # Exécution de la requête SELECT
        query = "SELECT id, payload, platform, date, is_processed, user_id FROM public.home_supportrequests where is_processed=false;"
        cursor.execute(query)

        # Récupération des résultats de la requête
        rows = cursor.fetchall()

        # Fermeture du curseur et de la connexion
        cursor.close()
        connection.close()

        # Création d'un DataFrame à partir des résultats
        df = pd.DataFrame(
            rows, columns=['id', 'payload', 'platform', 'date', 'is_processed', 'user'])

        return df

    except (Exception, Error) as error:
        # Gestion des erreurs de la base de données
        return error


@task
def launch_vpn():
    server = os.getenv('ANYCONNECT_SERVER')
    server_cert = os.getenv('ANYCONNECT_SERVER_CERT')
    user = os.getenv('ANYCONNECT_USER')
    password = os.getenv('ANYCONNECT_PASSWORD')

    # Construire la commande à exécuter
    command = f"openconnect {server} --servercert pin-sha256:{server_cert} --user={user} --key-password={password} <<EOF\n{password}\nEOF &"

    # Exécuter la commande en arrière-plan
    subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL)


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


@flow(task_runner=DaskTaskRunner())
def support():

    vpn_status = check_ip_availability.submit()
    vpn_status_result = vpn_status.result(raise_on_failure=False)

    if vpn_status.get_state().is_failed():

        vpn_start = launch_vpn.submit()

        get_data = get_support_request.submit(wait_for=[vpn_start])
        get_data_result = get_data.result(raise_on_failure=False)

    else:

        get_data = get_support_request.submit(wait_for=[vpn_start])
        get_data_result = get_data.result(raise_on_failure=False)

    for x in range(5):

        time.sleep(20)

        data_filtered_aaa = filtered_data.submit(
            get_data_result, "AAA", wait_for=[get_data])
        aaa_result = data_filtered_aaa.result(raise_on_failure=False)

        data_filtered_bscs = filtered_data.submit(
            get_data_result, "BSCS", wait_for=[get_data])
        bscs_result = data_filtered_bscs.result(raise_on_failure=False)

        data_filtered_dpi = filtered_data.submit(
            get_data_result, "DPI", wait_for=[get_data])
        dpi_result = data_filtered_dpi.result(raise_on_failure=False)

        data_filtered_elastic = filtered_data.submit(
            get_data_result, "ELASTIC", wait_for=[get_data])
        elastic_result = data_filtered_elastic.result(raise_on_failure=False)

        data_filtered_ocs = filtered_data.submit(
            get_data_result, "OCS", wait_for=[get_data])
        ocs_result = data_filtered_ocs.result(raise_on_failure=False)

        data_filtered_topup = filtered_data.submit(
            get_data_result, "TOPUP", wait_for=[get_data])
        topup_result = data_filtered_topup.result(raise_on_failure=False)

        if isinstance(aaa_result, pd.DataFrame) and not aaa_result.empty:
            a = print_aaa_data.submit(aaa_result, wait_for=[data_filtered_aaa])

        if isinstance(bscs_result, pd.DataFrame) and not bscs_result.empty:
            b = print_bscs_data.submit(
                bscs_result, wait_for=[data_filtered_bscs])

        if isinstance(dpi_result, pd.DataFrame) and not dpi_result.empty:
            c = print_dpi_data.submit(dpi_result, wait_for=[data_filtered_dpi])

        if isinstance(elastic_result, pd.DataFrame) and not elastic_result.empty:
            d = print_elastic_data.submit(
                elastic_result, wait_for=[data_filtered_elastic])

        if isinstance(ocs_result, pd.DataFrame) and not ocs_result.empty:
            e = print_ocs_data.submit(ocs_result, wait_for=[data_filtered_ocs])

        if isinstance(topup_result, pd.DataFrame) and not topup_result.empty:
            topup_prod_data = get_topup_prod_data.submit(
                topup_result, wait_for=[data_filtered_topup])
            topup_prod_data_result = topup_prod_data.result(
                raise_on_failure=False)

    return Completed()


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Support Script",
        flow=support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pools"
    )
    deployment.apply()

    support()
