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


@task
def print_aaa_data(data):

    print(data)


@task
def print_bscs_data(data):

    print(data)


@task
def print_dpi_data(data):

    print(data)


@task
def print_elastic_data(data):

    print(data)


@task
def print_ocs_data(data):

    print(data)


@task
def print_topup_data(data):

    print(data)


@task
def filtered_data(df):

    df_filtered_aaa = df.loc[df['platform'] == 'AAA']
    df_filtered_bscs = df.loc[df['platform'] == 'BSCS']
    df_filtered_dpi = df.loc[df['platform'] == 'DPI']
    df_filtered_elastic = df.loc[df['platform'] == 'ELASTIC']
    df_filtered_ocs = df.loc[df['platform'] == 'OCS']
    df_filtered_topup = df.loc[df['platform'] == 'TOPUP']

    return df_filtered_aaa, df_filtered_bscs, df_filtered_dpi, df_filtered_elastic, df_filtered_ocs, df_filtered_topup


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
        df = pd.DataFrame(rows)

        return df

    except (Exception, Error) as error:
        # Gestion des erreurs de la base de données
        return error


@flow(task_runner=DaskTaskRunner())
def support():

    get_data = get_support_request.submit()
    get_data_result = get_data.result(raise_on_failure=False)

    aaa, bscs, dpi, elastic, ocs, topup = filtered_data.submit(
        get_data_result, wait_for=[get_data])
    aaa_result, bscs_result, dpi_result, elastic_result, ocs_result, topup_result = filtered_data.result(
        raise_on_failure=False)

    a = print_aaa_data.submit(aaa_result, wait_for=[aaa])
    b = print_bscs_data.submit(aaa_result, wait_for=[bscs])
    c = print_dpi_data.submit(aaa_result, wait_for=[dpi])
    d = print_elastic_data.submit(aaa_result, wait_for=[elastic])
    e = print_ocs_data.submit(aaa_result, wait_for=[ocs])
    f = print_topup_data.submit(aaa_result, wait_for=[topup])


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Support Script",
        flow=support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pools"
    )
    deployment.apply()

    support()
