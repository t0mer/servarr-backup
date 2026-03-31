from abc import ABC, abstractmethod


class StorageBackend(ABC):
    @abstractmethod
    def list(self, prefix=""):
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, file_path, key):
        raise NotImplementedError

    @abstractmethod
    def delete_file(self, path):
        raise NotImplementedError

    @abstractmethod
    def cleanup(self, retention):
        raise NotImplementedError

    @abstractmethod
    def file_exists(self, path):
        raise NotImplementedError
