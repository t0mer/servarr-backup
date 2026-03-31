import io
import json
import logging
import os
import zipfile
from datetime import datetime, timedelta

import requests
import yaml

from ..destination import create_storage
from . import Server


# Configure logging
logger = logging.getLogger(__name__)


class Jackett(Server):
    def __init__(self, instance_name):
        config_dir = os.path.join(os.path.expanduser("~"), ".config", "servarr")
        config_path = os.path.join(config_dir, "config.yml")

        if not os.path.exists(config_path):
            raise FileNotFoundError("Configuration file not found. Please run 'servarr config init' first.")

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        instances = config.get("backups", {}).get("starrs", {}).get("jackett", [])
        jackett_config = next((inst for inst in instances if inst['name'] == instance_name), None)

        if not jackett_config:
            raise ValueError(f"Instance '{instance_name}' configuration not found in the configuration file.")

        dest_config = config.get("backups", {}).get("destination", {})
        url = jackett_config.get("url")
        api_key = jackett_config.get("api_key")

        if not url or not api_key:
            raise ValueError("Jackett URL or API Key is missing in the configuration.")

        super().__init__(url, api_key)

        self.instance_name = instance_name
        self.storage = create_storage(dest_config)


    def backup(self):
        # Fetch server config and indexers
        server_config = self._get_server_config()
        indexers = self._get_configured_indexers()

        if server_config is None and indexers is None:
            logger.error("Failed to retrieve any data from Jackett. Aborting backup.")
            return

        # Bundle into a zip file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        zip_filename = f"jackett_backup_{timestamp}.zip"
        zip_path = os.path.join("/tmp", zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            if server_config is not None:
                zf.writestr("server_config.json", json.dumps(server_config, indent=2))
            if indexers is not None:
                zf.writestr("indexers.json", json.dumps(indexers, indent=2))

        logger.info(f"Created Jackett backup archive at {zip_path}")

        # Upload to S3
        storage_key = f"jackett/{self.instance_name}/{zip_filename}"
        upload_success = self.storage.upload_file(zip_path, storage_key)

        if upload_success:
            os.remove(zip_path)
            logger.info(f"Deleted local backup file {zip_path} successfully.")
        else:
            logger.error(f"Failed to upload backup. Local file retained at {zip_path}.")


    def create_backup(self):
        # Jackett does not have a *arr-style backup command.
        # The backup() method handles fetching and bundling directly.
        self.backup()


    def _get_server_config(self):
        url = f"{self.url}/api/v2.0/server/config"
        params = {"apikey": self.api_key}
        logger.info("Fetching Jackett server config.")
        try:
            res = requests.get(url, params=params)
            if res.status_code == 200:
                logger.info("Retrieved Jackett server config successfully.")
                return res.json()
            else:
                logger.error(f"Failed to retrieve Jackett server config. Status code: {res.status_code}")
                return None
        except requests.RequestException as e:
            logger.error(f"Error fetching Jackett server config: {e}")
            return None


    def _get_configured_indexers(self):
        url = f"{self.url}/api/v2.0/indexers"
        params = {"apikey": self.api_key, "configured": "true"}
        logger.info("Fetching Jackett configured indexers.")
        try:
            res = requests.get(url, params=params)
            if res.status_code == 200:
                logger.info("Retrieved Jackett configured indexers successfully.")
                return res.json()
            else:
                logger.error(f"Failed to retrieve Jackett indexers. Status code: {res.status_code}")
                return None
        except requests.RequestException as e:
            logger.error(f"Error fetching Jackett indexers: {e}")
            return None


    def delete_backup(self, backup_name):
        logger.info(f"Deleting backup '{backup_name}' for Jackett.")
        try:
            self.storage.delete_file(backup_name)
            return True
        except Exception as e:
            logger.error(f"Failed to delete backup '{backup_name}': {e}")
            return False


    def delete_old_backups(self, retention_days):
        backups = self.list_backups()
        threshold_date = datetime.utcnow() - timedelta(days=retention_days)
        for backup in backups:
            obj_datetime = backup['LastModified']
            obj_datetime_native = obj_datetime.replace(tzinfo=None)

            if obj_datetime_native < threshold_date:
                self.delete_backup(backup['Key'])


    def list_backups(self):
        backups = self.storage.list("jackett")
        backup_list = []
        for backup in backups:
            backup_list.append({
                "Key": backup["Key"],
                "LastModified": backup["LastModified"],
                "Size": backup["Size"]
            })
        return backup_list


    def get_latest_backup(self):
        backups = self.list_backups()
        if not backups:
            return None
        return max(backups, key=lambda b: b['LastModified'])
