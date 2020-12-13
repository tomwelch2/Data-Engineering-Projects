![alt text](https://tdwi.org/articles/2020/03/23/-/media/TDWI/TDWI/BITW/AI17.jpg)
<h1>pandas_files</h1>
<h2>Pandas_pipeline1.py</h2>
Pipeline takes data from free API detailing cryptocurrency
and performs transformations and data-type casting. It is then
loaded into an AWS S3 bucket after the transformations have been applied.
<h2>pandas_introduction.ipynb</h2>
A Jupyter Notebook that gives a simple beginners guide into using Pandas for data
transformation and analysis

<h1>spark_files</h1>
<h2>pyspark_introduction.ipynb</h2>
An introductory Jupyter Notebook that explains the basics of using Pyspark for data
analysis/transformation. It explains how to load data into the Pyspark application,
how to specify a schema for the data and perform basic transformations such as column-dropping
and column creation - as well as aggregation, grouping and storing the data after the transformations
have occured.

<h2>pyspark_web_scraping.ipynb</h2>
A Jupyter Notebook that walks through a web-scraping application which utlises Pyspark for transforming 
and aggregating the data scraped from Corsair's website detailing RAM products. BeautifulSoup and Requests
is used to access and scrape the data from the website, where it is then loaded into a Pyspark DataFrame where
it is cleaned and transformed. Matplotlib is used to create a basic bar-chart displaying each product and its
corresponding price.

<h1>FlaskApis</h1>
<h2>flask_api1.py</h2>
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

<h2>dag4</h2>
ETL pipeline that takes data from MySQL using JDBC driver and spark, transforms it with spark and loads it into
an AWS S3 bucket.

In the transform stage, the data is grouped and aggregated - and columns containing dates as strings are casted
to dates. Unwanted columns are then dropped and the data is exported as a parquet file for storage.

In the load stage, the pipleine connects to an AWS S3 bucket using boto3 and stores the transformmed parquet
file in the bucket.

<h2>dag5</h2>
ETL pipeline that takes data from AWS S3 bucket, transforms it using Spark and saves it in a Snowflake database.

In the transform stage, the data is loaded in and unwanted columns are droped, grouping and aggregate functions
are also applied and their results are saved to a .csv file, with the main data also being exported as a .csv file
for loading.

In the load stage, the pipeline connects to a Snowflake db and the data is loaded into the database using sqlalchemy and Pandas.

<h1>R_files</h1>
<h2>r_pipeline.R</h2>
ETL pipeline written in R which takes data from an AWS S3 bucket and transforms/aggregates it and 
loads it into a MySQL database.

In the transform stage, columns with Dates stored as Strings are casted to Dates and grouping/aggregation
is performed.

In the load stage, the data is plotted as a barchart using ggplot and saved locally as a .png file. It is 
then loaded into a MySQL database using RMariaDB

<h1>dashDashboards</h1>
<h2>covid_19.py</h2>
Dashboard made using Dash and Plotly in python. Connects to free API relating to coronavirus cases around the world
and returns the data so it can be transformed using Pandas and represented in graph format using Plotly. The user can use
the dropdown menu to select which continent to filter by.























