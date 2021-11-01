import pyspark, airflow, os, datetime, apache_log_parser, pandas as pd
from pyspark.sql import SparkSession
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from minio import Minio

default_args = {
    'owner': 'Arya',
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'start_date': airflow.utils.dates.days_ago(1)}

client = Minio(
        "192.168.1.90:9199",
        access_key="arya",
        secret_key="minio123",
        secure=False)

line_parser = apache_log_parser.make_parser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")

def parsing_log(logs, nama_file) :
    log_list = []
    for log in logs :
        if log == '' :
            continue
        else :
            log = line_parser(log)
            data_log = []
            for key in keys :
                if key in list(log.keys()) :
                    if str(type(log[key])) != "<class 'NoneType'>" :
                        if key in ['time_received_datetimeobj', 'request_header_user_agent__is_mobile'] :
                            data_log.append(log[key])
                        else :
                            data_log.append(str(log[key]))
                    else :
                        data_log.append('')
                else :
                    if key in ['request_url_query_dict', 'request_url_query_simple_dict'] :
                        data_log.append(str({}))
                    elif key == 'request_url_query_list' :
                        data_log.append(str([]))
                    elif key == 'nama_file' :
                        data_log.append(nama_file)
                    else :
                        data_log.append('')
            log_list.append(data_log)
    return log_list

spark = SparkSession.builder \
        .appName('Test') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.90:9199") \
        .config("spark.hadoop.fs.s3a.access.key", "arya") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .getOrCreate()

keys = ['remote_host', 'remote_logname', 'remote_user', 'time_received', 'time_received_datetimeobj', 'time_received_isoformat',
        'time_received_tz_datetimeobj', 'time_received_tz_isoformat', 'time_received_utc_datetimeobj', 'time_received_utc_isoformat',
        'request_first_line', 'request_method', 'request_url', 'request_http_ver', 'request_url_scheme', 'request_url_netloc',
        'request_url_path', 'request_url_query', 'request_url_fragment', 'request_url_username', 'request_url_password', 
        'request_url_hostname', 'request_url_port', 'request_url_query_dict', 'request_url_query_list', 'request_url_query_simple_dict',
        'status', 'response_bytes_clf', 'request_header_referer', 'request_header_user_agent', 'request_header_user_agent__browser__family',
        'request_header_user_agent__browser__version_string', 'request_header_user_agent__os__family', 'request_header_user_agent__os__version_string',
        'request_header_user_agent__is_mobile','nama_file']

def check_log(ti, **kwargs) :
    log_file = os.listdir('/home/arya/log/')
    list_time_file = []
    for file in log_file :
        file_name = file.replace('access.log.','')
        list_time_file.append(file_name)
    list_time_file.sort(reverse=True)

    bucket_path_exists = False
    bucket_name = 'test'
    path = 'parsing_log'
    objects = client.list_objects(bucket_name)
    bucket_path = []
    for obj in objects:
        bucket_path.append(obj.object_name)
    if f'{path}/' in bucket_path :
        spark.sql(f"CREATE TABLE {path} USING DELTA LOCATION 's3a://{bucket_name}/{path}'")
        check_file = 3
        bucket_path_exists = True
        if len(list_time_file) > check_file :
            list_time_file = list_time_file[:check_file]
    input_log_list = []
    input = 0

    if bucket_path_exists == True :
        kolom = 'nama_file'
        query = f"SELECT {kolom} FROM {path} WHERE {kolom} IN {tuple(list_time_file)}"
        df = spark.sql(query).toPandas()

    for index in range(len(list_time_file)) : 
        print(f"Check File access.log.{list_time_file[index]}")
        text = open(f'/home/arya/log/access.log.{list_time_file[index]}').read()
        logs = text.split('\n')[:-1]
        if bucket_path_exists == True :
            df_time = df[df[kolom] == list_time_file[index]]
            jumlah_data = len(df_time)
            if len(logs) > jumlah_data :
                logs = logs[jumlah_data:]
                print(f'Input {len(logs)} data log from access.log.{list_time_file[index]}')
                input += 1
            else :
                print(f'No input data log from access.log.{list_time_file[index]}')
        else :
            input +=1
            print(f'Input {len(logs)} data log from access.log.{list_time_file[index]}')
        logs = parsing_log(logs, list_time_file[index])
        input_log_list.extend(logs)
        

    if input > 0 :
        df_spark = spark.createDataFrame(input_log_list,keys)
        df_spark.write.mode("append").format("delta").save(f"s3a://test/{path}")
        ti.xcom_push(key='jumlah_input', value=len(input_log_list))
        return ['input']
    else :
        return ['no_input']

def input(ti, **kwargs) :
    jumlah_input = ti.xcom_pull(key='jumlah_input', task_ids='check_log')
    print(f'Input {jumlah_input} data log to Minio')

def no_input(ti, **kwargs) :
    print(f'No Input data log to Minio')

def success(ti, **kwargs) :
    print(f'Parsing log file success')

dag = DAG('Parsing_Log', default_args=default_args, schedule_interval='@daily', catchup=False)

t1 = BranchPythonOperator(task_id='check_log', python_callable=check_log, provide_context=True, dag=dag)

t2 = PythonOperator(task_id='input', python_callable=input, provide_context=True, dag=dag)

t3 = PythonOperator(task_id='no_input', python_callable=no_input, provide_context=True, dag=dag)

t4 = PythonOperator(task_id='success', python_callable=success, provide_context=True, trigger_rule='all_success', dag=dag)

t1 >> [t2, t3] >> t4