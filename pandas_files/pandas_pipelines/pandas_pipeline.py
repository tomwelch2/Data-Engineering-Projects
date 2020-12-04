import pandas as pd
import boto3
import requests
import json

class apiConnector(object):
	def __init__(self, url, key, host):
		self.url = url
		self.key = key
		self.host = host


	@property
	def set_jsondata(self):
		'''Pulls data from free API found on rapidapi.com '''
		headers = {
    		'x-rapidapi-key': self.key, 
    		'x-rapidapi-host': self.host
    		}
		request = requests.get(self.url, headers = headers)
		jsondata = request.text
		self.jsondata = json.loads(jsondata)
		return self.jsondata

apiconnector = apiConnector(url = "https://coinpaprika1.p.rapidapi.com/exchanges",
			    key = "f2315dca5amshd0576ac8a8fc1a6p11bf84jsn8b7032c423ca",
			    host = "coinpaprika1.p.rapidapi.com")

data = apiconnector.set_jsondata #saving data returned from request in 'data' variable

df = pd.DataFrame(data) #using JSON data to create Pandas DataFrame
df.drop(['links', 'fiats', 'quotes'], axis = 1, inplace = True) #dropping columns with dictionaries as values
df.fillna(0, inplace = True)
title_descriptions = df[['name', 'description']] #creating seperate dataframe consisting of only name and description of currency
last_update_dates = df['last_updated'].str.split('T') #removing all but year-month-day to convert to date-type
last_update_dates = [i[0] for i in last_update_dates]
df['last_updated'] = last_update_dates
df['last_updared'] = pd.to_datetime(df['last_updated']) #converting last_updated column to datetime data-type

df.to_csv('/home/tom/Documents/csv_files/aws_api_data.csv')
table_descriptions.to_csv('/home/tom/Documents/csv_files/aws_table_descriptions.csv')

with open('/home/tom/AWS_CREDS.txt', 'r') as f:
	data = f.read() #reading in AWS credentials to connect to S3 bucket

data = data.split(' ') #splitting credentials into secret key and access key
aws_access_key_id = data[0]
aws_secret_access_key = data[1]


s3 = boto3.client(service_name = "s3", #connecting to S3 bucket
		  aws_access_key_id = aws_access_key_id,
		  aws_secret_access_key = aws_secret_access_key)

with open('/home/tom/Documents/csv_files/aws_api_data.csv', 'wb') as f:
	s3.upload_fileobj(f, 'testingbucket1003', 'api_data.csv') #uploading transformed dataframes
with open('/home/tom/Documents/csv_files/aws_table_descriptions.csv', 'wb') as f:
	s3.upload_fileobj(f, 'testingbucket1003', 'table_descriptions.csv')













