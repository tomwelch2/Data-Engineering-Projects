import pyspark.sql.functions as sql
import json
import boto3
from pymongo import MongoClient
from pyspark.sql import SparkSession



spark = SparkSession.builder.appName('pipeline').config("spark.ui.port", "4050").getOrCreate()

class Loader(object):
	def __init__(self, username: str, password: str, save_as: str, data) -> None:
		self.username = username
		self.password = password
		self._save_as = save_as
		self.data = data

	@property
	def save_as(self):
		return self._save_as

	@_save_as.setter
	def set_save_as(self, save_as):
		reserved_chars = [".", "/", "#"]
		if reserved_chars in save_as:
			raise ValueError("Error, Invalid character in filename")
		self._save_as = save_as




l1 = Loader('tomwelch2', "b", "gyugyug//.", 'iuyfy')
l1._save_as = "puhu/.."
print(l1.save_as)
