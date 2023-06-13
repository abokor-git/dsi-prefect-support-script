from prefect import task, flow
from prefect.server.schemas.states import Completed, Failed
from prefect.deployments import Deployment
from prefect_dask.task_runners import DaskTaskRunner

import os
import subprocess
from ping3 import ping
import psycopg2
from psycopg2 import Error
import pandas as pd

import time


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
def print_topup_data(data):
    time.sleep(20)
    print(data)


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
        query = "SELECT id, payload, platform, date, is_processed, user_id FROM public.home_supportrequests;"
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


@flow(task_runner=DaskTaskRunner())
def support():

    get_data = get_support_request.submit()
    get_data_result = get_data.result(raise_on_failure=False)

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
        b = print_bscs_data.submit(bscs_result, wait_for=[data_filtered_bscs])

    if isinstance(dpi_result, pd.DataFrame) and not dpi_result.empty:
        c = print_dpi_data.submit(dpi_result, wait_for=[data_filtered_dpi])

    if isinstance(elastic_result, pd.DataFrame) and not elastic_result.empty:
        d = print_elastic_data.submit(
            elastic_result, wait_for=[data_filtered_elastic])

    if isinstance(ocs_result, pd.DataFrame) and not ocs_result.empty:
        e = print_ocs_data.submit(ocs_result, wait_for=[data_filtered_ocs])

    if isinstance(topup_result, pd.DataFrame) and not topup_result.empty:
        f = print_topup_data.submit(
            topup_result, wait_for=[data_filtered_topup])


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Support Script",
        flow=support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pools"
    )
    deployment.apply()

    support()
