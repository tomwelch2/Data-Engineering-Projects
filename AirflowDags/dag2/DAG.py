import pyspark.sql.functions as sql
import pandas as pd
import boto3
from pyspark.sql import SparkSession
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#creating DAG --
args = {'owner': 'Tom'}
dag = DAG('MongoDB_DAG', schedule_interval = "59 * * * 0", start_date = days_ago(1),
	default_args = args)

#Retrieving AWS credentials
with open('/home/tom/AWS_CREDS.txt', 'r') as f:
	creds = f.read()

creds = creds.split(' ')
aws_access_key_id = creds[0].strip()
aws_secret_access_key = creds[1].strip()


def get_data():
	'''Connects to AWS S3 bucket and retrieves data regarding different
	  Pokemon'''
	s3 = boto3.client(service_name = 's3',
			  aws_access_key_id = aws_access_key_id,
			  aws_secret_access_key = aws_secret_access_key,
			  region_name = "eu-west-2")
	with open('/home/tom/Documents/csv_files/AWS_pokemon_data.csv', 'wb') as f:
		s3.download_fileobj('testingbucket1003', 'pokemon_data.csv', f)

get_data = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)


def transform_data():
	'''Loads in CSV file from AWS and drops unwanted columns, performs aggregation and saves
	   data to txt file and DataFrame to parquet file '''
	spark = SparkSession.builder.appName('aws_pipeline').config("spark.ui.port", "4050").getOrCreate()
	df = spark.read.csv('/home/tom/Documents/csv_files/AWS_pokemon_data.csv',
				inferSchema = True, header = True)
	df = df.dropna()
	columns_to_drop = ['Unnamed: 0_x', 'Unnamed: 0.1_x', 'Unnamed: 0_y', 'Unnamed: 0.1_y',
			   'Unnamed: 0.11', 'Unnamed: 0.1.1.1', 'Unnamed: 0.1.1.1.1'] #list of cols to drop from DataFrame
	df = df.drop(*columns_to_drop)
	df.createGlobalTempView('df')
	legend_data = spark.sql("SELECT COUNT(*) AS count FROM df GROUP BY Legendary;")
	df.write.parquet('/home/tom/Documents/csv_files/aws_pokemon_parquet.parquet') #writes df to parquet
	legend_data = legend_data.toPandas()
	legend_data = legend_data.to_dict(orient = 'records')
	with open('/home/tom/Documents/csv_files/aggregated_legend_data.txt', 'w') as f:
		f.write(json.dumps(legend_data, indent = 2)) #writes aggregate data to txt file

transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)

def load_data():
	'''Takes transformed parquet file and loads it into a MongoDB database '''
	df = pd.read_parquet('/home/tom/Documents/csv_files/aws_pokemon_parquet.parquet')
	df = df.to_dict(orient = 'records')
	client = MongoClient("mongodb+srv://tomwelch2:MongoDB123@clusterforyoutube.xyojo.mongodb.net/random_data?retryWrites=true&w=majority")
	db = client.get_database('random_data')
	records = db['random_data1'] #accesses set of records stored in database
	records.insert_many(df) #inserts data into records

load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)
remove_temp_parquet_file = BashOperator(task_id ='remove_temp_parquet_file', bash_command = """ 
cd /home/tom/Documents/csv_files && rm aws_pokemon_parquet.parquet
""")

get_data >> transform_data >> load_data >> remove_temp_parquet_file





