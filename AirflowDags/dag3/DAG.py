import pandas as pd
import pyspark.sql.functions as sql
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pymongo import MongoClient
from pyathena import connect
from pyathena.pandas.util import to_sql
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from ast import literal_eval

args = {'owner': 'Tom'}
dag = DAG('Athena_DAG', schedule_interval = "@daily", start_date = days_ago(1),
		default_args = args)

def get_data():
	'''Data resides in MongoDB database and relates to housing prices
	   in England'''
	client = MongoClient("mongodb+srv://tomwelch2:MongoDB123@clusterforyoutube.xyojo.mongodb.net/real_estate?retryWrites=true&w=majority")
	db = client.get_database('real_estate')
	records = db['house_prices']
	data = records.find({})
	data = [i for i in data]
	for i in data:
		del(i['_id'])

	with open('/home/tom/Documents/csv_files/mongodb_house_data.txt', 'w') as f:
		f.write(json.dumps(data, indent = 2))

get_data = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)


def transform_data():
	'''Transforms data loaded from MongoDB, changing 'Date' column to Date type and performing aggregation'''
	with open("/home/tom/Documents/csv_files/mongodb_house_data.txt", 'r') as f:
		data = f.read()
		data = literal_eval(data)

	spark = SparkSession.builder.appName('athena_pipeline').config("spark.ui.port", "4050").getOrCreate()
	schema = "_id STRING, Date STRING, region STRING, buyer_average FLOAT, average_invoice FLOAT, average_monthly FLOAT, average_annual FLOAT, occupier_average_price FLOAT, occupier_average_index FLOAT, occupier_average_monthly FLOAT, occupier_average_annual FLOAT"
	df = spark.createDataFrame(data, schema = schema)
	func = sql.udf(lambda x: datetime.strptime(x, "%d/%m/%Y"), DateType()) #function takes string date column and casts it to Date type
	df = df.withColumn('date', func(df.Date))
	df = df.drop('Date')
	groups = df.groupBy('region')
	df.createTempView('df')
	aggregate = spark.sql("SELECT region, AVG(buyer_average) AS average  FROM df GROUP BY region")
	aggregate.write.parquet('/home/tom/Documents/csv_files/house_aggregate_data.parquet')
	df.write.parquet('/home/tom/Documents/csv_files/house_parquet.parquet')


transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)


def load_data():
	'''Loads data into AWS Athena table'''
	with open('/home/tom/AWS_CREDS.txt', 'r') as f:
		data = f.read() #reads in AWS credentials

	aws_access_key_id = data.split(' ')[0].strip()
	aws_secret_access_key = data.split(' ')[1].strip()
	con = connect(s3_staging_dir="s3://testingbucket1003/",
                 region_name="eu-west-2",
                 aws_secret_access_key = aws_secret_access_key,
                 aws_access_key_id = aws_access_key_id)
	df = pd.read_parquet('/home/tom/Documents/csv_files/house_parquet.parquet')
	to_sql(df, 'house_data', con, "s3://testingbucket1003/athenadata/") #loads data into athena table


load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)




get_data >> transform_data >> load_data
