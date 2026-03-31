import logging
import os
import shutil
from datetime import datetime, timedelta

from .base import StorageBackend


class LocalStorage(StorageBackend):
    def __init__(self, path):
        self.base_path = os.path.expanduser(path)
        os.makedirs(self.base_path, exist_ok=True)

    def list(self, prefix=""):
        results = []
        search_dir = os.path.join(self.base_path, prefix) if prefix else self.base_path

        if not os.path.exists(search_dir):
            return results

        for root, dirs, files in os.walk(search_dir):
            for filename in files:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, self.base_path)
                stat = os.stat(full_path)
                results.append({
                    "Key": rel_path,
                    "LastModified": datetime.utcfromtimestamp(stat.st_mtime),
                    "Size": stat.st_size,
                })

        return results

    def upload_file(self, file_path, key):
        try:
            dest_path = os.path.join(self.base_path, key)
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(file_path, dest_path)
            logging.info(f"File copied to local storage as {key}")
            return True
        except Exception as e:
            logging.error(f"Error while copying {file_path} to local storage: {str(e)}")
            return False

    def delete_file(self, path):
        try:
            full_path = os.path.join(self.base_path, path)
            if os.path.exists(full_path):
                os.remove(full_path)
                logging.info(f"Deleted {path} from local storage.")
        except Exception as e:
            logging.error(f"Error while deleting file {path}: {str(e)}")

    def cleanup(self, retention):
        current_time = datetime.utcnow()
        retention_threshold = current_time - timedelta(days=retention)

        for item in self.list():
            if item["LastModified"] < retention_threshold:
                self.delete_file(item["Key"])

    def file_exists(self, path):
        full_path = os.path.join(self.base_path, path)
        return os.path.exists(full_path)
