import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import os
import fnmatch
import re
import logging
import io

client_error = ClientError

LOGGER =logging.getLogger()


class S3BucketManager(object):
    """
    Class BucketManager that uses the aws s3 api with boto3 to 
    manage an s3 Bucket.
    """
    
    def __init__(self, bucket_name:str, region:str=None, create_bucket=False, session:boto3.Session=None):
        """
        Instantiates a new S3BucketManager object. It creates the s3 client 
        and resource with boto3 and gets or creates an s3 bucket with the name
        given in the arguments

        Arguments:

            <session>: Expects an instantiated boto3.Session to use for the api calls. 
        """

        if not session:
            self.resource = boto3.resource("s3")
            self.client = boto3.client('s3')
        else:
            self.resource = session.resource("s3")
            self.client = session.client('s3')

        self.region = region if region else boto3.session.Session().region_name

        self.bucket = self.get_or_create_bucket_object(bucket_name, create_bucket=create_bucket)

    @staticmethod
    def parse_s3_uri(s3_uri:str):
        if not isinstance(s3_uri,str):
            raise TypeError("@s3_uri is not a string value.")
        if not s3_uri.startswith("s3://"):
            raise ValueError("@s3_uri must start with 's3://'")
        path = s3_uri.lstrip('s3://')
        psplit = path.split("/")
        bucket = psplit[0]
        fpath = "/".join(psplit[1:])
        return bucket, fpath

    def get_or_create_bucket_object(self, bucket_name, create_bucket=True):

        if hasattr(self, 'bucket') and self.bucket is not None:
            return self.bucket

        success = True

        if bucket_name not in [bucket.name for bucket in self.resource.buckets.all()]:

            if create_bucket:
                LOGGER.info("Creating bucket %s" % (bucket_name))
                if self.region == None:
                    success = self.create_bucket(bucket_name=bucket_name)
                else:
                    success = self.create_bucket(
                        bucket_name=bucket_name, region=self.region
                    )
            else:
                success = False

        if success:
            bucket = self.resource.Bucket(bucket_name)
            self.bucket_name = bucket.name
            return bucket

        logging.info(
            "Failed getting or creating bucket.\n"+
            "Make sure you're using the right credentials and permissions on your aws session.\n"+
            "If you have multiple profiles for your aws credentials file set the 'AWS_PROFILE' environment variable"+
            " with the name of the profile you want to use + 'ipython'. eg: 'AWS_PROFILE=\"development\"'"
        )
        info = ""
        if not create_bucket:
            info += " Use the argument 'create_bucket=True' to create the bucket if it doesn't exist."
        raise ValueError(
            f"Could not get bucket '{bucket_name}'."+info
        )

    @staticmethod
    def create_bucket(bucket_name, region=None):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (eg: us-east-1).

        @link See the aws s3 name of regions on: https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region == None:
                s3_client = boto3.client("s3")
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client("s3", region_name=region)
                location = {"LocationConstraint": region}
                s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )
        except ClientError as e:
            raise e

        return True
    

    def copy_to_s3(self, key_name:str, s3_uri:str):
        """
        Copies the file(s) in the given key to the given s3_uri 
        """
        ext_bucket_name, dst_path = self.parse_s3_uri(s3_uri)
        ext_bucket = self.resource.Bucket(ext_bucket_name)
        cp_src = {
            'Bucket':self.bucket_name,
            'Key':key_name
        }
        return ext_bucket.copy(cp_src, dst_path)


    def upload_raw_object(self, key_name, raw_data:bytes, metadata={}, storage_class="STANDARD", isfilelike=False):
        """
        Uploads an object given the raw binary data of the file. 

        The upload uses a custom configuration with threads, a 1024*25 multipart
        chunksize and threshold and a max of 10 concurrent processes for faster
        performance.

        Arguments:
            :key_name <str>: Path of the file in the s3 bucket.
            :raw_data <bytes>: Raw bytes data to upload.
            :metadata <dict>: Dictionary with extra metadata to add to the s3 object.
            :storage_class <str>: Can be one of:
                'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT

        Raise: <botocore.exceptions.ClientError>
        """
        config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)

        if isfilelike:
            fileobj = raw_data
        else:
            try:
                fileobj = io.BytesIO(raw_data)
            except TypeError:
                # expect TemporaryUploadedFile
                fileobj = io.BytesIO(raw_data.read())
        try:
            self.client.upload_fileobj(
                fileobj, self.bucket_name, key_name,
                ExtraArgs={
                    "StorageClass":storage_class,
                    "Metadata":metadata
                },
                Config=config,
            )
        except ClientError as e:
            logging.error(e)
            raise e

        return True


    def upload_file(self, local_filename, key_name, extra_args={}, storage_class="STANDARD"):
        """
        Arguments:
            :extra_args : More arguments to pass to the file upload, 
                eg: { 'ACL': 'public-read', 'ContentType': 'video/mp4'}
            :storage_class <str>: Can be one of:
                'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT
        """

        LOGGER.info("Trying to upload file %s to %s ..." % (local_filename, key_name))

        extra_args["StorageClass"] = storage_class

        self.client.upload_file(
            local_filename, self.bucket_name, key_name,
            ExtraArgs=extra_args
        )

        LOGGER.info("Done !")


    def download_file(self, key_name, local_filename="", ):
        if local_filename=="":
            local_filename = key_name.split("/")[-1] #Get name of file of the key.
        LOGGER.info("Downloading file %s to:  %s" % (key_name, local_filename))
        self.resource.meta.client.download_file(
            self.bucket_name, key_name, local_filename
        )
        LOGGER.info("Done !")


    def download_file_to_dir(self, key_name, local_dirname, local_filename=None):
        if local_filename == None:
            local_filename = key_name.split("/")[-1]

        if not os.path.isdir(local_dirname):
            os.makedirs(local_dirname)

        self.download_file(local_filename=os.path.join(local_dirname, local_filename), key_name=key_name)


    def download_files_on_dir(self, local_dir, prefix, suffix=''):

        if suffix:
            LOGGER.info("Downloading all files with prefix: %s and suffix: %s" % (prefix, suffix))
        else:
            LOGGER.info("Downloading all files with prefix: %s" % (prefix))

        dir_objects = [key for key in self.get_list_bucket_objects(prefix, suffix) if key[-1]!="/"]

        for obj in dir_objects:
            found_prefix = False
            local_file_path = []
            obj_prefix = "/".join(obj.split("/")[:-1])
            obj_name = obj.split("/")[-1]
            if obj_prefix == prefix:
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                local_filename = os.path.join(local_dir, obj_name)
                self.download_file(local_filename=local_filename, key_name=obj)
            else:
                object_path = []
                for file_path in obj.split("/"):
                    if "/".join(object_path) == prefix:
                        found_prefix = True
                        local_file_path.append(file_path)
                    else:
                        object_path.append(file_path)
                    
                local_filename = ""
                local_parent_dirs = ""
                for file_path in local_file_path:
                    if local_filename == "":
                        local_filename = os.path.join(local_filename, file_path)
                    else:
                        local_filename = os.path.join(
                            local_filename + os.sep, file_path
                        )
                    if file_path != local_file_path[-1]:
                        if local_parent_dirs == "":
                            local_parent_dirs = os.path.join(
                                local_parent_dirs, file_path
                            )
                        else:
                            local_parent_dirs = os.path.join(
                                local_parent_dirs + os.sep, file_path
                            )
                local_parent_dirs = os.path.join(local_dir + os.sep, local_parent_dirs)
                local_filename = os.path.join(local_dir + os.sep, local_filename)
                filename, ext = os.path.splitext(os.path.basename(local_filename))
                filename = re.sub("[^a-zA-Z\d-]", "", filename)
                local_filename = os.path.join(
                    local_parent_dirs + os.sep, filename + ext
                )
                if not os.path.isdir(local_parent_dirs):
                    os.makedirs(local_parent_dirs)
                self.download_file(local_filename=local_filename, key_name=obj)


    def list_objects(self, prefix='', suffix='', pattern='', **kwargs):
        """
        Returns a list of keys in the s3 bucket
        that match the prefix, suffix and glob pattern
        given.
        """
        objects = list(self.get_list_bucket_objects(prefix, suffix, **kwargs))
        if pattern:
            return fnmatch.filter(objects, pattern)
        return objects


    def get_list_bucket_objects(self, prefix='', suffix='', **kwargs):
        """
        Generate the keys in an S3 bucket. Returns a generator object.

        :param prefix: 
            Only fetch keys that start with this prefix (optional).
        :param suffix: 
            Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': self.bucket_name, **kwargs}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            resp = self.client.list_objects_v2(**kwargs)
            if not 'Contents' in resp:
                # Objects don't exist with that prefix or suffix:
                return []
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix) and key!="%s/"%(prefix):
                    yield key

            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break


    def generate_presigned_url_for_uploads(self, filename, key_prefix, expiration_seconds=3600, extra_params:dict={}, **kwargs):
        """
        Generates a presigned url to upload an object to s3 with a 'PUT' Http request.

        If success it returns the presigned url, otherwise it returns None.
        """
        key_prefix = key_prefix + "/" if not key_prefix.endswith("/") else key_prefix
        extra_params.update(
            {
                "Bucket":self.bucket_name,
                "Key": key_prefix + filename,
            }
        )
        try:
            presigned_url = self.client.generate_presigned_url(
                ClientMethod="put_object",
                Params=extra_params,
                ExpiresIn=expiration_seconds,
                HttpMethod="PUT",
                **kwargs
            )
        except ClientError as e:
            logging.error(e)
            return None
        return presigned_url

    def create_presigned_post(self, object_name,
                              fields=None, conditions=None, expiration_seconds=3600):
        """Generate a presigned URL S3 POST request to upload a file

        :param object_name: string
        :param fields: Dictionary of prefilled form fields
        :param conditions: List of conditions to include in the policy
        :param expiration_seconds: Time in seconds for the presigned URL to remain valid
        :return: Dictionary with the following keys:
            url: URL to post to
            fields: Dictionary of form fields and values to submit with the POST
        :return: None if error.
        """

        # Generate a presigned S3 POST URL
        try:
            response = self.client.generate_presigned_post(
                self.bucket_name,
                object_name,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=expiration_seconds
            )
        except ClientError as e:
            logging.error(e)
            return None
        # The response contains the presigned URL and required fields
        return response

    def copy_all_resources_to_new_bucket(self, bucket_name):
        for my_bucket_object in self.bucket.objects.all():
            LOGGER.info("copying: ", my_bucket_object.key)
            source = {"Bucket": self.bucket_name, "Key": my_bucket_object.key}
            self.resource.meta.client.copy(source, bucket_name, my_bucket_object.key)

    def delete_file(self, key_name):
        self.resource.meta.client.delete_object(Bucket=self.bucket_name, Key=key_name)

    def delete_multiple_files(self, object_keys:list, quietly=False):
        objects = [
            {'Key':key} for key in object_keys
        ]
        return self.client.delete_objects(
            Bucket=self.bucket_name,
            Delete={
                "Objects":objects,
                "Quiet":quietly
            }
        )

    def delete_files_with_prefix(self, prefix, **delete_kwargs):
        objects =  list(self.get_list_bucket_objects(prefix=prefix))
        return self.delete_multiple_files(objects, **delete_kwargs)

    def upload_large_file(self, local_filename, s3_key, extra_args={}, storage_class="STANDARD"):
        """
        Provides a finer grained method for the file upload to increase
        the performance of the file upload.

        Arguments:
            :extra_args : More arguments to pass to the file upload, 
                eg: { 'ACL': 'public-read', 'ContentType': 'video/mp4'}
            :storage_class <str>: Can be one of:
                'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT

        """
        config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)

        extra_args["StorageClass"] = storage_class

        self.client.upload_file(local_filename, self.bucket_name, s3_key,
            ExtraArgs=extra_args,
            Config=config,
        )

    def upload_folder(self, local_dir, key_prefix="", storage_class="STANDARD"):
        """
        Uploads a folder in the local machine to a prefix
        folder path in the s3 bucket.

        Arguments:
            :storage_class <str>: Can be one of:
                'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT
        """
        if key_prefix:
            if not key_prefix.endswith("/"):
                key_prefix = f"{key_prefix}/"

        for root, subdirs, files in os.walk(local_dir):
            directory_name = root.replace(local_dir,"").replace(os.sep, "/")
            for file in files:
                self.upload_file(os.path.join(root, file), f"{key_prefix}{directory_name}/{file}", storage_class=storage_class)


    @staticmethod
    def list_buckets(session:boto3.Session=None):

        if session:
            s3_resource = session.resource("s3")
        else:
            s3_resource = boto3.resource("s3")

        return list(s3_resource.buckets.all())

    @staticmethod
    def get_console_url_template():
        return "https://s3.console.aws.amazon.com/s3/buckets/{bucket_name}?region={region_name}&prefix={prefix}&showversions=false"

    def get_object_console_url(self, key:str):
        key = key.rstrip("/") + "/"
        temp = self.get_console_url_template()
        return temp.format(
            bucket_name=self.bucket_name,
            region_name=self.region,
            prefix=key
        )
