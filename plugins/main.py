from files import download_files
from postgres import load_file_in_db, execute_sql_file, Postgres_Credentials


def extract():
    download_files([{
        "url": "https://www.data.gouv.fr/fr/datasets/r/ac45ed59-7f4b-453a-9b3d-3124af470056",
        "dest_path": f"/opt/airflow/data/",
        "dest_name": "zone_alerte.csv",
    },
        {
            "url": "https://www.data.gouv.fr/fr/datasets/r/782aac32-29c8-4b66-b231-ab4c3005f574",
            "dest_path": f"/opt/airflow/data/",
            "dest_name": "arrete.csv",
        },
        {
            "url": "https://www.data.gouv.fr/fr/datasets/r/bbae8ea2-4d53-4f96-b7eb-9c08f66a07c5",
            "dest_path": f"/opt/airflow/data/",
            "dest_name": "departement.csv",
        }
    ])

def load():
    pg_credentials: Postgres_Credentials = {
        'pg_host': 'postgres-data',
        'pg_db': 'analytics',
        'pg_port': '5432',
        'pg_user': 'admin',
        'pg_password': 'admin',
        'pg_schema': 'public'
    }
    execute_sql_file(pg_credentials=pg_credentials, sql_file_path='/opt/airflow/plugins/schema/tables.sql')
    execute_sql_file(pg_credentials=pg_credentials, sql_file_path='/opt/airflow/plugins/schema/departement_geom.sql')

    load_file_in_db(pg_credentials=pg_credentials,csv_file_path='/opt/airflow/data/zone_alerte.csv', table_name='zone_alerte')
    load_file_in_db(pg_credentials=pg_credentials,csv_file_path='/opt/airflow/data/arrete.csv', table_name='arrete')



