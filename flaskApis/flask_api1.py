import pandas as pd
from sqlalchemy import create_engine
from flask import Flask
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)
engine = create_engine("mysql+pymysql://root:Gogos123@localhost:3306/my_company") #connecting to DB
SQL = "SELECT * FROM overclockers_data;"
df = pd.read_sql_query(SQL, con = engine) #returning SQL query results into Pandas DataFrame
dict_df = df.to_dict(orient = 'records') #converting data to dictionary to be returned in API

class pcMouseData(Resource):
	def get(self):
		return dict_df #returns all data from MySQL database

class postData(Resource):
	def post(self):
		parser = reqparse.RequestParser() #parser arguments given in URL to post data
		parser.add_argument("title", type = str, help = "No 'title' passed")
		parser.add_argument("price", type = int, help = "No 'price' passed")
		args = parser.parse_args()
		dict_df.append({'title': args['title'],
				'price': args['price']})
		return dict_df #returns data with posted record

api.add_resource(pcMouseData, '/') #adding endpoints for API
api.add_resource(postData, '/post') 
if __name__ == '__main__':
	app.run(debug = False) 
