import pyspark.sql.functions as sql
import matplotlib.pyplot as plt
import pandas as pd
import json
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from s3_operator import s3Operator
from sqlalchemy import create_engine

args = {'owner': 'Tom'}
dag = DAG('DAG6', schedule_interval = "@daily", start_date = days_ago(1), default_args = args)

with open('/home/tom/AWS_CREDS.txt', 'r') as f:
	creds = f.read()
	access_key = creds.split(' ')[0].strip()
	secret_key = creds.split(' ')[1].strip()

s3_download = s3Operator(task_id = 's3_operator', access_key = access_key, secret_access_key = secret_key,
			 bucket = "testingbucket1003", file = "electric.csv", filename_to_save_as = "electric_data.csv")

def transform_data():
	spark = SparkSession.builder.appName('pipeline').config("spark.ui.port", "4050").getOrCreate()
	schema = "reference STRING, period STRING, data_value INTEGER, suppressed STRING, status STRING, units STRING, magnitude INTEGER, subject STRING, group STRING, series_title_1 STRING, series_title_2 STRING, series_title_3 STRING, series_title_4 STRING, series_title_5 STRING"
	df = spark.read.csv('electric_data.csv', schema = schema)
	cols_to_drop = ['series_title_3', "series_title_4", "series_title_5", "group", "supressed", "subject"]
	df = df.drop(*cols_to_drop)
	groups = df.groupBy('series_title_2')
	agg = groups.agg({'data_value': 'mean'})
	agg = agg.withColumnRenamed('avg(mean)', "mean_value")
	agg_rdd = agg.rdd
	dict_agg = agg_rdd.map(lambda x: x.asDict()).take(10)
	with open('/home/tom/Documents/csv_files/electric_agg.txt', 'w') as f:
		f.write(json.dumps(dict_agg, indent = 2))
	splits = sql.split(df.period, '.')
	df = df.withColumn('Period', splits.getItem(0))
	df = df.drop('period')
	df = df.toPandas()
	df.to_csv('/home/tom/Documents/csv_files/electric_transformed.csv')

transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)

def load_data():
	with open('/home/tom/sql_password_json.json', 'r') as f:
		sql_pword = json.load(f)

	sql_pword = sql_pword['password']
	engine = create_engine("mysql+pymysql://root:{}@localhost:3306/my_company".format(sql_pword))
	df = pd.read_csv('/home/tom/Documents/csv_files/electric_transformed.csv')
	with open('/home/tom/Documents/csv_files/electric_agg.txt', 'r') as f:
		agg_data = json.load(f)

	agg_df = pd.DataFrame(agg_data)
	plt.figure(figsize = (20, 10))
	plt.bar(agg_df['series_title_2'], df['mean_value'], color = "green")
	plt.title("Average Value By Sector")
	plt.ylabel("Value ($)")
	plt.savefig('/home/tom/Documents/csv_files/electric_graph.png')
	df.to_sql("electric_s3_data", con = engine)

load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)

s3_download >> transform_data >> load_data

