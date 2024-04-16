import pandas as pd

from helpers.files import download_files
from helpers.postgres import load_file_in_db, execute_sql_file, Postgres_Credentials


def extract():
    download_files([{
        "url": "https://www.data.gouv.fr/fr/datasets/r/ac45ed59-7f4b-453a-9b3d-3124af470056",
        "dest_path": f"data/",
        "dest_name": "zone_alerte.csv",
    },
    {
        "url": "https://www.data.gouv.fr/fr/datasets/r/782aac32-29c8-4b66-b231-ab4c3005f574",
        "dest_path": f"data/",
        "dest_name": "arrete.csv",
    },
    ])


def transform_analytics_arrete(df_arretes: pd.DataFrame) -> pd.DataFrame:
    df_analytics_arrete = df_arretes[
        ['numero_arrete', 'date_signature', 'debut_validite_arrete', 'fin_validite_arrete', 'numero_niveau']].drop_duplicates()
    df_analytics_arrete['duree_jours'] = (pd.to_datetime(df_analytics_arrete['fin_validite_arrete'], errors='coerce') - pd.to_datetime(
        df_analytics_arrete['debut_validite_arrete'], errors='coerce')).dt.days + 1
    df_analytics_arrete = df_analytics_arrete[df_analytics_arrete['duree_jours'] > 0]
    return df_analytics_arrete

def transform_analytics_niveau(df_arretes: pd.DataFrame, df_zone_alerte: pd.DataFrame) -> pd.DataFrame:
    df_analytics_niveau = df_arretes[['id_zone', 'debut_validite_arrete', 'fin_validite_arrete', 'numero_niveau']]
    df_analytics_niveau['fin_validite_arrete'] = pd.to_datetime(df_analytics_niveau['fin_validite_arrete'], errors='coerce')
    df_analytics_niveau['debut_validite_arrete'] = pd.to_datetime(df_analytics_niveau['debut_validite_arrete'], errors='coerce')
    df_analytics_niveau = df_analytics_niveau[df_analytics_niveau['debut_validite_arrete']<=df_analytics_niveau['fin_validite_arrete']]
    df_analytics_niveau = df_analytics_niveau.dropna(subset=['fin_validite_arrete', 'fin_validite_arrete'])

    # Duplication des intervalles par jour
    df_analytics_niveau['date_jour'] = df_analytics_niveau[['debut_validite_arrete', 'fin_validite_arrete']].apply(
        lambda x: pd.date_range(x['debut_validite_arrete'], x['fin_validite_arrete']), axis=1)

    df_analytics_niveau = df_analytics_niveau.explode('date_jour').drop(['debut_validite_arrete', 'fin_validite_arrete'], axis=1)
    df_analytics_niveau = df_analytics_niveau.merge(df_zone_alerte, on='id_zone', how='inner').drop(['id_zone'], axis=1)
    df_analytics_niveau = df_analytics_niveau[
        ['code_iso_departement', 'date_jour', 'numero_niveau', 'surface_zone', 'type_zone']].reset_index(drop=True)
    return df_analytics_niveau


def transform():
    df_arretes = pd.read_csv('data/arrete.csv', dtype=object)
    df_zone_alerte = pd.read_csv('data/zone_alerte.csv', dtype=object)

    df_analytics_arrete = transform_analytics_arrete(df_arretes)
    df_analytics_niveau = transform_analytics_niveau(df_arretes, df_zone_alerte)

    df_analytics_arrete.to_csv('data/analytics_arrete.csv', index=False)
    df_analytics_niveau.to_csv('data/analytics_niveau.csv', index=False)


def load():
    pg_credentials: Postgres_Credentials = {
        'pg_host': 'postgres-data',
        'pg_db': 'analytics',
        'pg_port': '5432',
        'pg_user': 'admin',
        'pg_password': 'admin',
        'pg_schema': 'public'
    }
    execute_sql_file(pg_credentials=pg_credentials, sql_file_path='plugins/schema/data_tables.sql')
    execute_sql_file(pg_credentials=pg_credentials, sql_file_path='plugins/schema/analytics_tables.sql')
    # execute_sql_file(pg_credentials=pg_credentials, sql_file_path='plugins/schema/departement_geom.sql')

    load_file_in_db(pg_credentials=pg_credentials, csv_file_path='data/zone_alerte.csv', table_name='zone_alerte')
    load_file_in_db(pg_credentials=pg_credentials, csv_file_path='data/arrete.csv', table_name='arrete')

    load_file_in_db(pg_credentials=pg_credentials, csv_file_path='data/analytics_arrete.csv', table_name='analytics_arrete')
    load_file_in_db(pg_credentials=pg_credentials, csv_file_path='data/analytics_niveau.csv', table_name='analytics_niveau')



