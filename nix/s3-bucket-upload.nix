{ config, lib, uuid, name, ... }:

with lib;

{

  options = {

    uploadName = mkOption {
      type = types.str;
      description = "Short description of this upload.";
    };

    bucketName = mkOption {
      default = "charon-${uuid}-${name}";
      type = types.str;
      description = "Name of the S3 bucket to upload to.";
    };

    source = mkOption {
      type = types.str;
      description = "Local file or folder path";
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


  };

}
