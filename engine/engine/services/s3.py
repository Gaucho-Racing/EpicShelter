import time
import boto3
from botocore.exceptions import ClientError
import logging

class S3Service:
    def __init__(self, bucket_name: str, access_key_id: str, secret_access_key: str):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)

    def upload_file(self, file_path: str, s3_path: str) -> bool:
        try:
            start_time = time.time()
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            upload_time = time.time() - start_time
            self.logger.info(f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_path} in {upload_time:.2f} seconds")
            return True
        except ClientError as e:
            self.logger.error(f"Failed to upload {file_path}: {str(e)}")
            return False

    def upload_parquet(self, parquet_path: str, s3_path: str) -> bool:
        if not parquet_path.endswith('.parquet'):
            self.logger.error(f"File {parquet_path} is not a parquet file")
            return False
            
        return self.upload_file(parquet_path, s3_path)

    def delete_files(self, s3_path: str) -> bool:
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except ClientError as e:
            self.logger.error(f"Failed to delete {s3_path}: {str(e)}")
            return False