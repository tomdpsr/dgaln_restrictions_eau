#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### DAG Tutorial Documentation
This DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

from __future__ import annotations

# [START tutorial]
# [START import_module]

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from main import extract, load

# The DAG object; we'll need this to instantiate a DAG


# [END import_module]

# [START instantiate_dag]
with DAG(
    "dag_restrictions_eau",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    #default_args={"retries": 2},
    # [END default_args]
    description="DAG Mise à jour des données de restrictions eau",
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 13, tz="UTC"),
    catchup=False,
   # tags=[""],
) as dag:

    # [START main_flow]
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_task >> load_task
