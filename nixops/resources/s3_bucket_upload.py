# -*- coding: utf-8 -*-

# Automatic provisioning of AWS S3 buckets.

import time
import boto.s3.connection
import nixops.util
import nixops.resources
import nixops.ec2_utils
import os
import subprocess

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
        self.upload_id = xml.find("attrs/attr[@name='uploadId']/string").get("value")
        self.bucket_name = xml.find("attrs/attr[@name='bucketName']/string").get("value")
        self.source_directory = xml.find("attrs/attr[@name='sourceDirectory']/string").get("value")
        self.source_package = xml.find("attrs/attr[@name='sourcePackage']/string").get("value")
        self.bucket_folder = xml.find("attrs/attr[@name='bucketFolder']/string").get("value")
        self.region = xml.find("attrs/attr[@name='region']/string").get("value")
        self.access_key_id = xml.find("attrs/attr[@name='accessKeyId']/string").get("value")


    def show_type(self):
        return "{0} [{1}]".format(self.get_type(), self.region)


class S3BucketUploadState(nixops.resources.ResourceState):
    """State of an S3 bucket upload."""

    state = nixops.util.attr_property("state", nixops.resources.ResourceState.MISSING, int)
    upload_id = nixops.util.attr_property("ec2.uploadId", None)
    bucket_name = nixops.util.attr_property("ec2.bucketName", None)
    source_directory = nixops.util.attr_property("ec2.sourceDirectory", None)
    source_package = nixops.util.attr_property("ec2.sourcePackage", None)
    bucket_folder = nixops.util.attr_property("ec2.bucketFolder", None)
    region = nixops.util.attr_property("ec2.region", None)
    access_key_id = nixops.util.attr_property("ec2.accessKeyId", None)

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
        return self.upload_id

    def get_definition_prefix(self):
        return "resources.s3BucketUploads."

    def connect(self, bucket_name):
        if self._conn: return
        (access_key_id, secret_access_key) = nixops.ec2_utils.fetch_aws_secret_key(self.access_key_id)

        if '.' in bucket_name:
          self._conn = boto.s3.connection.S3Connection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, calling_format=boto.s3.connection.OrdinaryCallingFormat())
        else:
          self._conn = boto.s3.connection.S3Connection(aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    def build_package(self, source_package):
        """Build the S3 upload configurations in the Nix store."""

        self.log("building configurations for S3 upload...")

        try:
          result_path = subprocess.check_output(
                 ["nix-build"] + [source_package],
                 #stderr=self.logger.log_file
                 ).rstrip()
        except subprocess.CalledProcessError:
          raise Exception("unable to build all machine configurations")

        return result_path

    def determine_actual_source_directory(self, source_package, source_directory):
        #use result_path/source_directory (if using a source_package). otherwise just use source_directory
        if not source_package is None:
          result_path = self.build_package(source_package)
          if not os.path.exists(result_path):
            raise Exception("source package for bucket upload '{0}' does not exist".format(result_path))
          source_directory = result_path + "/" + source_directory
        return source_directory

    def get_file_list(self, source_directory, bucket_folder):
        self.log("get file list from '{0}' to bucket dest ‘{1}’...".format(source_directory, bucket_folder))

        #make certain there is a valid source_directory
        if not os.path.exists(source_directory):
            raise Exception("source directory for bucket upload '{0}' does not exist".format(source_directory))

        # get the list of fully qualified file paths in the source directory and subdirectories
        source_file_names = []
        for (source_dir, dir_names, file_names) in os.walk(source_directory):
            for fn in file_names:
                source_file_names.append(os.path.join(source_dir, fn))

        source_dest_pairs = []

        #preserve subdirectory nesting from the original location.
        for source_file in source_file_names:
            dest_file = source_file.replace(source_directory, bucket_folder)
            source_dest_pairs.append((source_file,dest_file))
            self.log("'{0}' to ‘{1}’".format(source_file, dest_file))

        return source_dest_pairs


    def file_exists_in_bucket(self, bucket, file_key):
        try:
            self._conn.head_object(Bucket=bucket, Key=file_key)
            return True
        except:
            return False

    def perform_upload(self, bucket, defn):
        source_package = defn.source_package
        source_dir = defn.source_directory

        #max size in bytes before uploading in parts. between 1 and 5 GB recommended
        MAX_SIZE = 20 * 1000 * 1000
        #size of parts when uploading in parts
        PART_SIZE = 6 * 1000 * 1000

        actual_source_dir = self.determine_actual_source_directory(source_package, source_dir);
        self.log("perform upload '{0}' to bucket dest ‘{1}’...".format(actual_source_dir, defn.bucket_folder))

        source_dest_pairs = self.get_file_list(actual_source_dir, defn.bucket_folder);

        def percent_cb(complete, total):
            self.log('.')

        for (source_path, dest_path) in source_dest_pairs:
            if self.file_exists_in_bucket(bucket, dest_path):
               continue

            self.log("Uploading '{0}' to Amazon S3 bucket".format(source_path))

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

    def create_after(self, resources, defn):
        return {r for r in resources if
                isinstance(r, nixops.resources.s3_bucket.S3BucketState)}


    def create(self, defn, check, allow_reboot, allow_recreate):

        self.access_key_id = defn.access_key_id or nixops.ec2_utils.get_access_key_id()
        if not self.access_key_id:
            raise Exception("please set ‘accessKeyId’, $EC2_ACCESS_KEY or $AWS_ACCESS_KEY_ID")

        if len(defn.bucket_name) > 63:
            raise Exception("bucket name ‘{0}’ is longer than 63 characters.".format(defn.bucket_name))

        if check or self.state != self.UP:

            self.connect(defn.bucket_name)

            self.log("uploading '{0}' to S3 bucket ‘{1}’...".format(defn.source_package + "/" + defn.source_directory, defn.bucket_name))

            bucket = self._conn.get_bucket(defn.bucket_name)

            self.perform_upload(bucket, defn)

            with self.depl._db:
                self.state = self.UP
                self.upload_id = defn.upload_id
                self.bucket_name = defn.bucket_name
                self.source_directory = defn.source_directory
                self.source_package = defn.source_package
                self.bucket_folder = defn.bucket_folder
                self.region = defn.region
                self.access_key_id = defn.access_key_id

    def destroy(self, wipe=False):
        if self.state == self.UP:
            self.connect(self.bucket_name)
            try:
                self.log("destroying S3 bucket upload ‘{0}’...".format(self.upload_id))
                bucket = self._conn.get_bucket(self.bucket_name)
                bucketListResultSet = bucket.list(prefix=self.bucket_folder)
                bucket.delete_keys([key.name for key in bucketListResultSet])

            except boto.exception.S3ResponseError as e:
                if e.error_code != "NoSuchBucket": raise
        return True

def region_to_s3_location(self, region):
    # S3 location names are identical to EC2 regions, except for
    # us-east-1 and eu-west-1.
    if region == "eu-west-1": return "EU"
    elif region == "us-east-1": return ""
    else: return region
