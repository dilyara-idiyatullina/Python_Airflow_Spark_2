import datetime
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_input_filename(dt, i):
    current_dt = dt - datetime.timedelta(days=i)
    return f"{current_dt.strftime('%Y-%m-%d')}.csv"


def generate_output_file():

    dt = datetime.datetime.now().date()

    input_dir_name = "/opt/airflow/input"
    prev_days_cnt = 7  # период, за который формируем данные (неделя)
    input_filenames_list = [
        os.path.join(input_dir_name, generate_input_filename(dt, i))
        for i in range(1, prev_days_cnt + 1)
    ]

    df_list = []

    for filename in input_filenames_list:
        if os.path.isfile(filename):
            try:
                df_list.append(pd.read_csv(filename, header=None))
            except pd.errors.EmptyDataError:
                print(filename, "is empty file")

    if df_list:
        big_frame = pd.concat(df_list, ignore_index=True)
        big_frame.columns = ["email", "action", "dt"]
    else:
        big_frame = pd.DataFrame(columns=["email", "action", "dt"])

    output_frame = pd.pivot_table(
        big_frame,
        index=["email"],
        columns=["action"],
        values="action",
        aggfunc="count",
        fill_value=0,
    )

    output_frame = output_frame.reindex(columns=["CREATE", "READ", "UPDATE", "DELETE"])
    output_frame = output_frame.rename(
        columns={
            "CREATE": "create_count",
            "READ": "read_count",
            "UPDATE": "update_count",
            "DELETE": "delete_count",
        }
    )

    output_dir_name = "/opt/airflow/output"
    if not os.path.isdir(output_dir_name):
        os.mkdir(output_dir_name)
    filepath = os.path.join(output_dir_name, f"{dt.strftime('%Y-%m-%d')}.csv")

    output_frame.to_csv(filepath)


dag_1 = DAG(
    "simple_python_dag",
    start_date=datetime.datetime(2024, 9, 23),
    schedule="0 7 * * *",
)

python_task = PythonOperator(
    task_id="python_task", python_callable=generate_output_file, dag=dag_1
)

python_task
