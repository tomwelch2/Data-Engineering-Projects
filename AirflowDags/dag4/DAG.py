import pandas as pd
import pyspark.sql.functions as sql
import findspark
import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


args = {'owner': 'Tom'}
dag = DAG('DAG4', schedule_interval = '@daily', start_date = days_ago(1))

def get_data():
	findspark.add_packages('mysql:mysql-connector-java:8.0.11')
	spark = SparkSession.builder.appName('pipeline').getOrCreate()
	with open('/home/tom/mysql_creds.json', 'r') as f:
		data = json.load(f)

	hostname = 'localhost'
	jdbcPort = 3306
	password = data['password']
	dbname = 'my_company'
	jdbc_url = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(hostname,jdbcPort, dbname,username,password)
	query = "(select * from syria_data) t1_alias"
	df = spark.read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=jdbc_url, dbtable=query).load()
	df.write.parquet('/home/tom/Documents/csv_files/syria_parquet.parquet')

get_data = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)



def transform_data():
	spark = SparkSession.builder.appName('pipeline').getOrCreate()
	df = spark.read.parquet('/home/tom/Documents/csv_files/syria_parquet.parquet')
	groups = df.groupBy('Actors')
	count = groups.count()
	count = count.toPandas()
	count.to_csv('/home/tom/Documents/csv_files/syria_count.csv')
	df = df.drop('count')
	splits  = sql.split(df.Date, '-')
	df = df.withColumn('date', splits.get_item(0))
	df = df.drop('Date')
	df.write.parquet('/home/tom/Documents/csv_files/transformed_syria_data.parquet')

transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)

def load_data():
	with open('/home/tom/AWS_CREDS.txt') as f:
		creds = f.read()
	aws_access_key_id = creds.split(' ')[0].strip()
	aws_secret_access_key = creds.split(' ')[1].strip()
	s3 = boto3.client(service_name = "s3",
			  aws_access_key_id = aws_access_key_id,
			  aws_secret_access_key = aws_secret_access_key)
	with open('/home/tom/Documents/csv_files/transformed_syria_data.parquet', 'wb') as f:
		s3.upload_fileobj('testingbucket1003', 'syria_parquet.parquet', f)

load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)
remove_temp_files = BashOperator(task_id = 'remove_temp_files', bash_command = """ 
cd /home/tom/Documents/csv_files && rm -r transformed_syria_data.parquet
""")


get_data >> [transform_data, load_data] >> remove_temp_files
