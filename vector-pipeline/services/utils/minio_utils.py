from minio import Minio

class MinioHandler:
    def __init__(self, endpoint, access_key, secret_key, secure=False):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    
    def list_files(self, bucket_name):
        objects = self.client.list_objects(bucket_name)
        return [obj.object_name for obj in objects]
    
    def get_file_content(self, bucket_name, filename):
        response = self.client.get_object(bucket_name, filename)
        return response.read().decode("utf-8")
