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
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""

from __future__ import annotations


from pprint import pprint

import pendulum
from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id = 'dag_python_operator_with_deco',
    schedule = '30 6 * * *', # 매일 6시 30분
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:

    @task(task_id="python_task_1")
    def print_context(some_input):
        """Print the Airflow context and ds variable from the context."""
        print(some_input)

    python_task_1 = print_context("task deco is 실행이예요")
    
    
   

