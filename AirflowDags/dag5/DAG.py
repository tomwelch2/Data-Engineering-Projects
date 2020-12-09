import pyspark.sql.functions as sql
import pandas as pd
import boto3
import json
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine

args = {'owner': 'Tom'}
dag = DAG('DAG4', schedule_interval = "@daily", start_date = days_ago(1),
		default_args = args)
#AWS Credentials ---
with open('/home/tom/AWS_CREDS.txt', 'r') as f:
	creds = f.read()

aws_access_key_id = creds.split(' ')[0].strip()
aws_secret_access_key = creds.split(' ')[1].strip()

def get_data():
	s3 = boto3.client(service_name = "s3",
			  region_name = "eu-west-2",
			  aws_access_key_id = aws_access_key_id,
			  aws_secret_access_key = aws_secret_access_key)
	with open('/home/tom/Documents/csv_files/aws_sales_records.csv', 'wb') as f:
		s3.download_fileobj('testingbucket1003', 'sales_records.csv', f) #downloading data from AWS S3 bucket

get_data = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)



def transform_data():
	spark = SparkSession.builder.appName('pipeline').getOrCreate()
	schema = "region STRING, country STRING, item_type STRING, sales_channel STRING, order_priority STRING, orderdate STRING, order_id INTEGER, ship_date STRING, units_sold INTEGER, unit_price FLOAT, unit_cost FLOAT, total_revenue FLOAT, total_cost FLOAT, total_profit FLOAT"
	df = spark.read.csv('/home/tom/Documents/csv_files/aws_sales_records.csv', header = True, schema = schema)
	df = df.withColumn('order_date', sql.to_date(df.orderdate, 'MM-dd-yyyy')) #adding transformed date column
	df = df.drop('orderdate') #dropping string date column
	groups = df.groupBy('region')
	aggregate_data = groups.agg({'total_profit': 'mean'})
	aggregate_data = aggregate_data.toPandas()
	aggregate_data.to_csv('/home/tom/Documents/csv_files/aws_sales_agg.csv') #exporting aggregate data to .csv
	df = df.toPandas()
	df.to_csv('/home/tom/Documents/csv_files/aws_records_csv.csv')

transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)

def load_data():
	with open('/home/tom/sql_password_json.json', 'r') as f:
		data = json.load(f)
		pword = data['password'] #extracting password from JSON file

	sfAccount = 'cx42816.eu-west-1'
	sfUser = "tomwelch2"
	sfPswd = pword
	engine = create_engine(f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/demo_db") #creating a connection between Python and Snowflake
	con = engine.connect()
	df = pd.read_csv('/home/tom/Documents/csv_files/aws_records_csv.csv') #reading in transformed parquet file
	df = df.iloc[:10000]
	df.to_sql("aws_sales_data", con = engine, index = False, schema = 'public',
		index_label = None) #sending to Snowflake db
	df.to_csv('/home/tom/Documents/csv_files/aws_sales_csv.csv')

load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)
r_script = BashOperator(task_id = 'r_script', bash_command = """ 
cd /home/tom/Documents/python_files/git_project/AirflowDags/dag5 && Rscript r_script.R
""", dag = dag)
bash_script = BashOperator(task_id = "Bash_script", bash_command = """ 
cd /home/tom/Documents/python_files/git_project/AirflowDags/dag5 && ./bash_script.sh
""", dag = dag)


get_data >> transform_data >> load_data  >> r_script >> bash_script



get_data >> transform_data >> load_data
