{ config, lib, uuid, name, ... }:

with lib;

{

  options = {

    name = mkOption {
      type = types.str;
      description = "Name of the S3 bucket upload.";
    };

    bucketName = mkOption {
      default = "charon-${uuid}-${name}";
      type = types.str;
      description = "Name of the S3 bucket to upload to.";
    };

    region = mkOption {
      type = types.str;
      description = "Amazon S3 region.";
    };

    accessKeyId = mkOption {
      type = types.str;
      description = "The AWS Access Key ID.";
    };

    arn = mkOption {
      type = types.str;
      description = "Amazon Resource Name (ARN) of the S3 bucket. This is set by NixOps.";
    };

    renameToUID = mkOption {
      type = types.bool;
      description = "Rename the uploaded file or folder to a unique identifier.";
    }
  };

}
