import psycopg2
from typing import TypedDict
import os

class Postgres_Credentials(TypedDict):
    pg_host: str
    pg_port: str
    pg_db: str
    pg_user: str
    pg_password: str
    pg_schema: str


def execute_sql_file(
    pg_credentials: Postgres_Credentials,
    sql_file_path: str
):
    is_file = os.path.isfile(sql_file_path)
    if is_file:
        conn = psycopg2.connect(
            host=pg_credentials['pg_host'],
            port=pg_credentials['pg_port'],
            database=pg_credentials['pg_db'],
            user=pg_credentials['pg_user'],
            password=pg_credentials['pg_password'],
            options=f"--search_path={pg_credentials['pg_schema']}"
        )
        with conn.cursor() as cur:
            cur.execute(
                open(
                    os.path.join(sql_file_path), "r"
                ).read()
            )
            conn.commit()
            conn.close()
    else:
        raise Exception(
            f"file {sql_file_path} does not exists"
        )


def load_file_in_db(
    pg_credentials: Postgres_Credentials,
    csv_file_path: str,
    table_name: str
):
    is_file = os.path.isfile(os.path.join(csv_file_path))
    if is_file:
        conn = psycopg2.connect(
            host=pg_credentials['pg_host'],
            port=pg_credentials['pg_port'],
            database=pg_credentials['pg_db'],
            user=pg_credentials['pg_user'],
            password=pg_credentials['pg_password'],
            options=f"-c search_path={pg_credentials['pg_schema'],}"
        )
        file = open(
            os.path.join(csv_file_path), "r"
        )
        with conn.cursor() as cur:
            cur.copy_expert(
                sql=(
                    f"COPY {pg_credentials['pg_schema']}.{table_name} FROM STDIN "
                    f"WITH CSV HEADER DELIMITER AS ','"
                ),
                file=file,
            )
            conn.commit()
            conn.close()
    else:
        raise Exception(
            f"file {csv_file_path} does not exists"
        )
