{ config, lib, uuid, name, ... }:

with lib;
{

  options = {

    uploadId = mkOption {
      default = "s3upload-${uuid}-${name}";
      type = types.str;
      description = "The name of this s3 bucket upload";
    };

    bucketName = mkOption {
      type = types.str;
      description = "Name of the S3 bucket to upload to.";
    };

    bucketFolder = mkOption {
      type = types.str;
      description = "Name of the S3 bucket folder to upload to.";
    };

    sourceDirectory = mkOption {
      type = types.str;
      description = "Local file or folder path. Alternatively a subdirectory from the sourcePackage";
    };

    sourcePackage = mkOption {
      type = types.str;
      description = "Optional Location of Nix package to build and upload";
    };

    region = mkOption {
      type = types.str;
      description = "Amazon S3 region.";
    };

    accessKeyId = mkOption {
      type = types.str;
      description = "The AWS Access Key ID.";
    };

  };

  config = {
    _type = "s3-bucket-upload";
  };

}
