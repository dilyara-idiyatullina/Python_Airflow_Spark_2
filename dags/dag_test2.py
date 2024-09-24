import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from typing import List
from py4j.java_gateway import JavaObject

TMP_OUTPUT_DIR = "TMP_CSV_PARTS"


def configure_hadoop(spark: SparkSession):
    hadoop = spark.sparkContext._jvm.org.apache.hadoop  # type: ignore
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    return hadoop, conf, fs


def ensure_exists(spark: SparkSession, file: str):
    hadoop, _, fs = configure_hadoop(spark)
    if not fs.exists(hadoop.fs.Path(file)):
        out_stream = fs.create(hadoop.fs.Path(file, False))
        out_stream.close()


def delete_location(spark: SparkSession, location: str):
    hadoop, _, fs = configure_hadoop(spark)
    if fs.exists(hadoop.fs.Path(location)):
        fs.delete(hadoop.fs.Path(location), True)


def get_files(spark: SparkSession, src_dir: str) -> List[JavaObject]:
    """Get list of files in HDFS directory"""
    hadoop, _, fs = configure_hadoop(spark)
    ensure_exists(spark, src_dir)
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))

    return files


def copy_merge_into(
    spark: SparkSession, src_dir: str, dst_file: str, delete_source: bool = True
):
    """Merge files from HDFS source directory into single destination file

    Args:
        spark: SparkSession
        src_dir: path to the directory where dataframe was saved in multiple parts
        dst_file: path to single file to merge the src_dir contents into
        delete_source: flag for deleting src_dir and contents after merging

    """
    hadoop, conf, fs = configure_hadoop(spark)

    # 1. Get list of files in the source directory
    files = get_files(spark, src_dir)

    # 2. Set up the 'output stream' for the final merged output file
    # if destination file already exists, add contents of that file to the output stream
    if fs.exists(hadoop.fs.Path(dst_file)):
        tmp_dst_file = dst_file + ".tmp"
        tmp_in_stream = fs.open(hadoop.fs.Path(dst_file))
        tmp_out_stream = fs.create(hadoop.fs.Path(tmp_dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(
                tmp_in_stream, tmp_out_stream, conf, False
            )  # False means don't close out_stream
        finally:
            tmp_in_stream.close()
            tmp_out_stream.close()

        tmp_in_stream = fs.open(hadoop.fs.Path(tmp_dst_file))
        out_stream = fs.create(hadoop.fs.Path(dst_file), True)
        try:
            hadoop.io.IOUtils.copyBytes(tmp_in_stream, out_stream, conf, False)
        finally:
            tmp_in_stream.close()
            fs.delete(hadoop.fs.Path(tmp_dst_file), False)
    # if file doesn't already exist, create a new empty file
    else:
        out_stream = fs.create(hadoop.fs.Path(dst_file), False)

    # 3. Merge files from source directory into the merged file 'output stream'
    try:
        for file in files:
            in_stream = fs.open(file)
            try:
                hadoop.io.IOUtils.copyBytes(
                    in_stream, out_stream, conf, False
                )  # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()

    # 4. Tidy up - delete the original source directory
    if delete_source:
        delete_location(spark, src_dir)


def generate_output_file():

    spark = SparkSession.builder.appName("demo").getOrCreate()

    dt = datetime.datetime.now().date()

    input_dir_name = "/opt/airflow/input"
    prev_days_cnt = 7  # период, за который формируем данные (неделя)

    input_filenames_list = []
    for i in range(1, prev_days_cnt + 1):
        current_dt = dt - datetime.timedelta(days=i)
        filename = os.path.join(
            input_dir_name, f"{current_dt.strftime('%Y-%m-%d')}.csv"
        )
        if os.path.isfile(filename) and os.stat(filename).st_size != 0:
            input_filenames_list.append(filename)

    try:
        big_frame = (
            spark.read.options(header=False, delimiter=",")
            .csv(input_filenames_list)
            .toDF("email", "action", "dt")
        )
    except:
        schema = StructType(
            [
                StructField("email", StringType(), True),
                StructField("action", StringType(), True),
                StructField("dt", DateType(), True),
            ]
        )
        big_frame = spark.createDataFrame([], schema)
        print("Error: can't union input files.")

    big_frame.createOrReplaceTempView("users_with_actions")

    sql_str = (
        "SELECT email, "
        "COUNT(CASE WHEN action = 'CREATE' THEN action END) AS create_count, "
        "COUNT(CASE WHEN action = 'READ' THEN action END) AS read_count, "
        "COUNT(CASE WHEN action = 'UPDATE' THEN action END) AS update_count, "
        "COUNT(CASE WHEN action = 'DELETE' THEN action END) AS delete_count "
        "FROM users_with_actions "
        "GROUP BY email"
    )

    output_frame = spark.sql(sql_str)

    output_dir_name = "output"
    if not os.path.isdir(output_dir_name):
        os.mkdir(output_dir_name)
    filepath = os.path.join(output_dir_name, f"{dt.strftime('%Y-%m-%d')}.csv")

    if os.path.isfile(filepath):
        os.remove(filepath)

    #### >>>>>>> Так как по условию задачи требуется получившийся агрегированный файл класть
    # в директорию output и именовать файл по правилу YYYY-MM-DD.csv, а spark при сохранении
    # делит результат на части, то пришлось воспользоваться готовым решением с
    # https://engineeringfordatascience.com/posts/how_to_save_pyspark_dataframe_to_single_output_file/

    output_frame = output_frame.repartition(5)

    # write headers first (required for csv only)
    headers = spark.createDataFrame(
        data=[[f.name for f in output_frame.schema.fields]],
        schema=StructType(
            [
                StructField(f.name, StringType(), False)
                for f in output_frame.schema.fields
            ]
        ),
    )

    headers.write.csv(TMP_OUTPUT_DIR)

    # write csv headers to output file first
    copy_merge_into(
        spark,
        TMP_OUTPUT_DIR,
        filepath,
        delete_source=True,
    )

    # Write main outputs
    # dataframe written to TMP_OUTPUT_DIR folder in 5 separate csv files (one for each partition)
    output_frame.write.csv(TMP_OUTPUT_DIR)

    # merge main csv files in folder into single file
    copy_merge_into(
        spark,
        TMP_OUTPUT_DIR,
        filepath,
        delete_source=True,
    )

    #### <<<<<<<<<<


dag_1 = DAG(
    "simple_python_dag2",
    start_date=datetime.datetime(2024, 9, 23),
    schedule="0 7 * * *",
)

python_task = PythonOperator(
    task_id="python_task", python_callable=generate_output_file, dag=dag_1
)

python_task
