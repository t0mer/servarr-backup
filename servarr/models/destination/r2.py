import logging

import boto3

from .s3 import S3Bucket


class R2Bucket(S3Bucket):
    def __init__(self, account_id, bucket_name, access_key, secret_key):
        endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def upload_file(self, file_path, s3_key):
        try:
            # R2 does not support ACL, so we omit ExtraArgs
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logging.info(f"File uploaded to R2 successfully as {s3_key}")
            return True
        except Exception as e:
            logging.error(f"Error while uploading {file_path} to R2: {str(e)}")
            return False
