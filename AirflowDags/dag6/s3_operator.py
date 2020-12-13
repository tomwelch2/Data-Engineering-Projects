import boto3
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class s3Operator(BaseOperator):
	@apply_defaults
	def __init__(self, access_key: str, secret_access_key: str, bucket: str, file: str,
			filename_to_save_as: str, *args, **kwargs) -> None:
		super().__init__(*args, **kwargs)
		self.access_key = access_key
		self.secret_access_key = secret_access_key
		self.bucket = bucket
		self.file = file
		self.filename_to_save_as = filename_to_save_as

	def execute(self, context):
		s3 = boto3.client(service_name = "s3",
				aws_access_key_id = self.access_key,
				aws_secret_access_key = self.secret_access_key,
				region_name = 'eu-west-2')
		with open(self.filename_to_save_as, 'wb') as f:
			s3.download_fileobj(self.bucket, self.file, f)



