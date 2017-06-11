{ config, lib, uuid, name, ... }:

with lib;
{

  options = {

    uploadId = mkOption {
      default = "s3upload-${uuid}-${name}";
      type = types.str;
      description = "Short description of this upload.";
    };

    bucketName = mkOption {
      type = types.str;
      description = "Name of the S3 bucket to upload to.";
    };

    sourceDirectory = mkOption {
      type = types.str;
      description = "Local file or folder path. Alternatively a subdirectory from the sourcePackage";
    };

    sourcePackage = mkOption {
      type = types.str;
      description = "Optional Location of Nix package to build and upload";
    };

    bucketDestination = mkOption {
      type = types.str;
      description = "Bucket file key (remote path).";
    };

    renameToUID = mkOption {
      type = types.bool;
      description = "Rename the uploaded file or folder to a unique identifier.";
    };

    region = mkOption {
      type = types.str;
      description = "Amazon S3 region.";
    };

    accessKeyId = mkOption {
      type = types.str;
      description = "The AWS Access Key ID.";
    };


  }; #// import ./common-ec2-options.nix { inherit lib; };

  #config = {
  #  _type = "s3-bucket-upload";
  #};

}
