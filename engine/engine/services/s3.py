import time
import boto3
from botocore.exceptions import ClientError

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

    def upload_file(self, file_path: str, s3_path: str) -> bool:
        try:
            start_time = time.time()
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            upload_time = time.time() - start_time
            print(f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_path} in {upload_time:.2f} seconds")
            return True
        except ClientError as e:
            print(f"Failed to upload {file_path}: {str(e)}")
            return False

    def upload_parquet(self, parquet_path: str, s3_path: str) -> bool:
        if not parquet_path.endswith('.parquet'):
            print(f"File {parquet_path} is not a parquet file")
            return False
            
        return self.upload_file(parquet_path, s3_path)
