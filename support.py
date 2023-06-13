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
def print_data(data):

    print(data)


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
    result = get_data.result(raise_on_failure=False)

    y = print_data.submit(result, wait_for=[get_data])


if __name__ == "__main__":

    deployment = Deployment.build_from_flow(
        name="Support Script",
        flow=support,
        work_queue_name="agent-prod",
        work_pool_name="xana-pools"
    )
    deployment.apply()

    support()
