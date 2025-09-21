import apache_beam as beam
from minio import Minio

class ReadFromMinio(beam.PTransform):
    def __init__(self, endpoint, access_key, secret_key, bucket, secure=False):
        super().__init__()
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.secure = secure

    def expand(self, pcoll):
        # Get filenames first (this runs on the main process)
        temp_client = Minio(self.endpoint, access_key=self.access_key, secret_key=self.secret_key, secure=self.secure)
        files = list(temp_client.list_objects(self.bucket))
        filenames = [obj.object_name for obj in files]
        
        return (
            pcoll
            | beam.Create(filenames)
            | beam.ParDo(self._ReadFileDoFn(self.endpoint, self.access_key, self.secret_key, self.bucket, self.secure))
        )

    class _ReadFileDoFn(beam.DoFn):
        def __init__(self, endpoint, access_key, secret_key, bucket, secure):
            self.endpoint = endpoint
            self.access_key = access_key
            self.secret_key = secret_key
            self.bucket = bucket
            self.secure = secure
            self.client = None

        def setup(self):
            # Create client per worker - called once per worker initialization
            self.client = Minio(
                self.endpoint, 
                access_key=self.access_key, 
                secret_key=self.secret_key, 
                secure=self.secure
            )

        def process(self, filename):
            data = self.client.get_object(self.bucket, filename).read().decode("utf-8")
            yield (filename, data)

        def teardown(self):
            # Clean up if needed
            self.client = None
