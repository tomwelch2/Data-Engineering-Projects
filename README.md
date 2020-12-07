![alt text](https://tdwi.org/articles/2020/03/23/-/media/TDWI/TDWI/BITW/AI17.jpg)
<h1>pandas_files</h1>
<h2>Pandas_pipeline1</h2>
Pipeline takes data from free API detailing cryptocurrency
and performs transformations and data-type casting. It is then
loaded into an AWS S3 bucket after the transformations have been applied.
<h2>pandas_introduction</h2>
A Jupyter Notebook that gives a simple beginners guide into using Pandas for data
transformation and analysis

<h1>FlaskApis</h1>
<h2>flask_api1</h2>
REST API created with Flask that takes web-scraped data regarding gaming PC mouses 
from MySQL database and returns it in JSON format. An endpoint for posting data 
to the API has also been implemented.

<h1>AirflowDags</h1>
<h2>dag1</h2>
ETL Pipeline created with Apache Airflow and Pyspark. The pipeline recieves the data
via web-scraping and cleans it, before exporting it as a JSON file for transformation.

In the transformation stage, the data is cleaned further and data-type conversions are 
applied using Pyspark to allow for aggregation. The transformed data is then written to a parquet file.

In the loading stage, the data is read in from the parquet file and is loaded into a MySQL database through
Pyspark, so that it can be accessed by the R script that creates visualisations based on the transformed data
and stores it locally.

<h2>dag2</h2>
ETL Pipeline created with Apache Airflow, Pandas and Pyspark. The data, which concerns different
pokemon, is loaded into the pipeline from an AWS S3 bucket.

The transformation stage loads the CSV file downloaded from the S3 bucket and transforms it using Pyspark
(removing NULL values, dropping unwanted columns, performing aggregation) - where the aggregate data is exported
as a JSON file and the main DataFrame as a parquet file for loading.

In the loading stage, the data is read from the parquet file and is transformed to a JSON object, where it is
then loaded into a NoSQL MongoDB cluster.

<h2>dag3</h2>
ETL pipeline made with Apache Airflow, Pandas and Pyspark that recieves the data from a NoSQL MongoDB cluster
and transforms it using Pyspark, before loading it into an AWS Athena DB.

In the transformation stage, the data is loaded in from a JSON file containing the data extracted from the MongoDB
cluster, where Pyspark is used to transform the data (changing column data-types, aggregating, removing unwanted
columns) - it is then exported as a parquet file for loading.

In the loading stage, the pipeline loads the transformed parquet file and connects to a AWS Athena DB,
where it is then loaded.
























