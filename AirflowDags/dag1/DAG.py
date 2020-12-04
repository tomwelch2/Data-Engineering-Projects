import requests
import json
import findspark
import pyspark.sql.functions as sql
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago


args = {'owner': 'Tom'}
dag = DAG('DAG1', schedule_interval = "@daily", start_date = days_ago(1),
	default_args = args) #instantiating DAG

create_database = MySqlOperator(task_id = 'create_database', mysql_conn = "mysql_default",
sql = "CREATE DATABASE IF NOT EXISTS dag_data;", dag = dag)

def get_data(): #scrapes and cleans data from website
	URL = "http://books.toscrape.com/"
	request = requests.get(URL)
	content = request.content #HTML content of website
	soup = BeautifulSoup(content, 'lxml')
	title_data = soup.find_all('article')
	price_data = soup.find_all('div', class_ = "product_price")
	titles = [i.h3.text for i in title_data] #grabbing h3 text from article tags
	titles = [i.replace('\n', '') for i in titles] #replacing new-line characters 
	prices = [i.text for i in price_data]
	prices = [i.replace('\n', '') for i in prices]
	prices = [i.replace('Add to basket', '') for i in prices]
	prices = [i.replace('In stock', '') for i in prices]
	prices = [i.strip() for i in prices] #stipping whitespace from price lists
	data = [{'title': i, 'price': j} for i, j in zip(titles, prices)]
	with open('/home/tom/Documents/csv_files/book_data.txt', 'w') as f:
		f.write(json.dumps(data, indent = 2))

get_data = PythonOperator(task_id = 'get_data', python_callable = get_data, dag = dag)

def transform_data():
	spark = SparkSession.builder.appName('pipeline').config("spark.ui.port", "4050").getOrCreate()
	with open('/home/tom/Documents/csv_files/book_data.txt', 'r') as f:
		data = json.load(f) #loading data as python dictionary

	schema = "title STRING, price STRING"
	df = spark.createDataFrame(data, schema = schema)
	df = df.select('title', sql.regexp_replace(df.price, '£', '').alias('price')) #removing £ symbols
	df = df.select('title', df.price.cast(FloatType().alias('price'))) #casting price to float datatype
	df.write.parquet('/home/tom/Documents/csv_files/book_parquet.parquet')

transform_data = PythonOperator(task_id = 'transform_data', python_callable = transform_data, dag = dag)

def load_data():
	findspark.add_packages('mysql:mysql-connector-java:8.0.11') #adding JDBC driver to connect to MySQL
	spark = SparkSession.builder.appName('pipeline').config("spark.ui.port", "4050").getOrCreate()
	hostname = "localhost"
	dbname = "dag_data"
	jdbcPort = 3306
	username = "root"
	password = "Gogos123"
	jdbc_url = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(hostname,jdbcPort, dbname,username,password)
	df = spark.read.parquet('/home/tom/Documents/csv_files/book_parquet.parquet')
	df.write.format('jdbc').options(url=jdbc_url,
                                        driver='com.mysql.jdbc.Driver',
      					dbtable='bookstore_data',
      					user=username,
      					password=password).mode('overwrite').save()) #saving to MySQL

load_data = PythonOperator(task_id = 'load_data', python_callable = load_data, dag = dag)
remove_temp_json = BashOperator(task_id = 'remove_temp_json', command = """ 
cd /home/tom/Documents/csv_files && rm book_data.txt
""", dag = dag)

run_r_script = BashOperator(task_id = 'run_r_script', python_callable = run_r_script,
command = """ 
cd /home/tom/Documents/python_files/git_project/AirflowDags/dag1 && Rscript dag_r_script.R
""", dag = dag)












