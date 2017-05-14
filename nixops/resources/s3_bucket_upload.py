# -*- coding: utf-8 -*-

# Automatic provisioning of AWS S3 buckets.

import time
import boto.s3.connection
import nixops.util
import nixops.resources
import nixops.ec2_utils


class S3BucketUploadDefinition(nixops.resources.ResourceDefinition):
    """Definition of an S3 bucket upload."""

    @classmethod
    def get_type(cls):
        return "s3-bucket-upload"

    @classmethod
    def get_resource_type(cls):
        return "s3BucketUploads"

    def __init__(self, xml):
        nixops.resources.ResourceDefinition.__init__(self, xml)
        self.bucket_name = xml.find("attrs/attr[@name='name']/string").get("value")
        self.bucket_upload_name = xml.find("attrs/attr[@name='name']/string").get("value")
        self.region = xml.find("attrs/attr[@name='region']/string").get("value")
        self.access_key_id = xml.find("attrs/attr[@name='accessKeyId']/string").get("value")
        self.policy = xml.find("attrs/attr[@name='policy']/string").get("value")

    def show_type(self):
        return "{0} [{1}]".format(self.get_type(), self.region)


class S3BucketUploadState(nixops.resources.ResourceState):
    """State of an S3 bucket upload."""

    state = nixops.util.attr_property("state", nixops.resources.ResourceState.MISSING, int)
    bucket_name = nixops.util.attr_property("ec2.bucketName", None)
    bucket_upload_name = nixops.util.attr_property("ec2.bucketUploadName", None)
    access_key_id = nixops.util.attr_property("ec2.accessKeyId", None)
    rename_to_uid = nixops.util.attr_property("ec2.policy", None)
    region = nixops.util.attr_property("ec2.region", None)


    @classmethod
    def get_type(cls):
        return "s3-bucket-upload"


    def __init__(self, depl, name, id):
        nixops.resources.ResourceState.__init__(self, depl, name, id)
        self._conn = None


    def show_type(self):
        s = super(S3BucketUploadState, self).show_type()
        if self.region: s = "{0} [{1}]".format(s, self.region)
        return s


    @property
    def resource_id(self):
        return self.bucket_upload_name

    def get_definition_prefix(self):
        return "resources.s3BucketUploads."

    def connect(self):
        if self._conn: return
        (access_key_id, secret_access_key) = nixops.ec2_utils.fetch_aws_secret_key(self.access_key_id)
        self._conn = boto.s3.connection.S3Connection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    def create_after(self, resources, defn):
        return {r for r in resources if
                isinstance(r, nixops.resources.s3_bucket.S3BucketState)}


    def create(self, defn, check, allow_reboot, allow_recreate):

        self.access_key_id = defn.access_key_id or nixops.ec2_utils.get_access_key_id()
        if not self.access_key_id:
            raise Exception("please set ‘accessKeyId’, $EC2_ACCESS_KEY or $AWS_ACCESS_KEY_ID")

        if len(defn.bucket_upload_name) > 63:
            raise Exception("bucket upload name ‘{0}’ is longer than 63 characters.".format(defn.bucket_upload_name))

        if check or self.state != self.UP:

            self.connect()

            self.log("creating S3 bucket upload ‘{0}’...".format(defn.bucket_upload_name))
            try:
                self._conn.create_bucket(defn.bucket_upload_name, location=region_to_s3_location(defn.region))
            except boto.exception.S3CreateError as e:
                if e.error_code != "BucketAlreadyOwnedByYou": raise

            bucket = self._conn.get_bucket(defn.bucket_upload_name)
            if defn.policy:
                self.log("setting S3 bucket policy on ‘{0}’...".format(bucket))
                bucket.set_policy(defn.policy.strip())
            else:
                try:
                    bucket.delete_policy()
                except boto.exception.S3ResponseError as e:
                    # This seems not to happen - despite docs indicating it should:
                    # [http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEpolicy.html]
                    if e.status != 204: raise # (204 : Bucket didn't have any policy to delete)

            with self.depl._db:
                self.state = self.UP
                self.bucket_upload_name = defn.bucket_upload_name
                self.region = defn.region
                self.policy = defn.policy


    def destroy(self, wipe=False):
        if self.state == self.UP:
            self.connect()
            try:
                self.log("destroying S3 bucket ‘{0}’...".format(self.bucket_upload_name))
                bucket = self._conn.get_bucket(self.bucket_upload_name)
                try:
                    bucket.delete()
                except boto.exception.S3ResponseError as e:
                    if e.error_code != "BucketNotEmpty": raise
                    if not self.depl.logger.confirm("are you sure you want to destroy S3 bucket ‘{0}’?".format(self.bucket_upload_name)): return False
                    keys = bucket.list()
                    bucket.delete_keys(keys)
                    bucket.delete()
            except boto.exception.S3ResponseError as e:
                if e.error_code != "NoSuchBucket": raise
        return True


def region_to_s3_location(self, region):
    # S3 location names are identical to EC2 regions, except for
    # us-east-1 and eu-west-1.
    if region == "eu-west-1": return "EU"
    elif region == "us-east-1": return ""
    else: return region

def get_file_list(self, source_directory,bucket_dest_dir):
    sourceFileNames = []
    for (source_dir, dir_names, file_names) in os.walk(source_directory):
        for fn in file_names
        source_fileN_nmes.append(os.path.join(source_dir, fn))

    source_dest_pairs = []

    #preserve subdirectory nesting from the original location.
    for source_file in source_file_names
        dest_file = source_file.replace(source_directory, bucket_dest_dir)
        source_dest_pairs.append((source_file,dest_file))

    return source_dest_pairs


def file_exists_in_bucket(self, bucket, file_key):
    try:
        self._conn.head_object(Bucket=bucket, Key=file_key)
        return True
    except:
        return False

def perform_upload(self, bucket_name, source_dir, bucket_dest_dir):
    # Fill in info on data to upload
    # destination bucket name
    #bucket_name = 'jwu-testbucket'
    # source directory
    #sourceDir = 'testdata/'
    # destination directory name (on s3)
    #bucketDestDir = ''

    #max size in bytes before uploading in parts. between 1 and 5 GB recommended
    MAX_SIZE = 20 * 1000 * 1000
    #size of parts when uploading in parts
    PART_SIZE = 6 * 1000 * 1000

    bucket = self._conn.get_bucket(bucket_name)

    source_dest_pairs = self.get_file_list(source_dir, bucket_dest_dir);

    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    for (source_path, dest_path) in source_dest_pairs:
        if self.file_exists_in_bucket(bucket, dest_path):
           continue

        self.log("Uploading '{0}' to Amazon S3 bucket {1}".format(source_path, bucket_name))

        filesize = os.path.getsize(source_path)
        if filesize > MAX_SIZE:
            self.log("multipart upload")
            mp = bucket.initiate_multipart_upload(dest_path)
            fp = open(source_path,'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                self.log("uploading part {0}".format(fp_num))
                mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

            mp.complete_upload()

        else:
            self.log("singlepart upload")
            k = boto.s3.key.Key(bucket)
            k.key = dest_path
            k.set_contents_from_filename(source_path, cb=percent_cb, num_cb=10)
