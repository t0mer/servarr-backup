from .s3 import S3Bucket
from .r2 import R2Bucket
from .local import LocalStorage


def create_storage(config):
    """
    Factory function that creates the appropriate storage backend
    based on the destination config.

    The config is the 'destination' dict from backups config, e.g.:
        destination:
          type: s3
          s3:
            endpoint: ...
    """
    dest_type = config.get("type", "s3")

    if dest_type == "s3":
        s3_config = config.get("s3", {})
        return S3Bucket(
            s3_config.get("endpoint"),
            s3_config.get("bucket"),
            s3_config.get("key", {}).get("access"),
            s3_config.get("key", {}).get("secret"),
        )
    elif dest_type == "r2":
        r2_config = config.get("r2", {})
        return R2Bucket(
            r2_config.get("account_id"),
            r2_config.get("bucket"),
            r2_config.get("key", {}).get("access"),
            r2_config.get("key", {}).get("secret"),
        )
    elif dest_type == "local":
        local_config = config.get("local", {})
        return LocalStorage(
            local_config.get("path"),
        )
    else:
        raise ValueError(f"Unknown destination type: {dest_type}")
