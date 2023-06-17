from prefect import task, flow
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment
from prefect_dask.task_runners import DaskTaskRunner

import mysql.connector
import psycopg2
from psycopg2 import Error

import os
import subprocess

import pandas as pd
import time


@task
def xana_save_data(data):

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

        raise Failed()


@task
def topup_get_prod_data(data):

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
        raise Failed()

    return data


@task
def xana_get_topup_support_request():

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
        query = "SELECT id, payload, platform, date, is_processed, user_id FROM public.home_supportrequests where platform='TOPUP' and is_processed=false;"
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
        raise Failed()


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

    time.sleep(3)


@flow(task_runner=DaskTaskRunner())
def topup_support():

    vpn_start = launch_vpn.submit()

    while (True):

        try:

            xana_get_data = xana_get_topup_support_request.submit(wait_for=[
                                                                  vpn_start])
            xana_get_data_result = xana_get_data.result(raise_on_failure=True)

            if isinstance(xana_get_data_result, pd.DataFrame) and not xana_get_data_result.empty:

                topup_prod_get_data = topup_get_prod_data.submit(
                    xana_get_data_result, wait_for=[xana_get_data])
                topup_prod_get_data_result = topup_prod_get_data.result(
                    raise_on_failure=True)

                topup_save_data = xana_save_data.submit(
                    topup_prod_get_data_result, wait_for=[topup_prod_get_data])
                topup_save_data = topup_save_data.result(
                    raise_on_failure=True)

        except Exception as e:

            return Failed()

        time.sleep(10)


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Topup Support Script",
        flow=topup_support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pools"
    )
    deployment.apply()

    topup_support()
