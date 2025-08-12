from azure.storage.blob import BlobServiceClient
from airflow.sdk import Connection
from typing import Optional
from azure.identity import DefaultAzureCredential

class BlobManager:
    def __init__(self, connection_string: Optional[str] = None, account_url: Optional[str] = None, container_name: Optional[str] = None):
        """
        Initializes the BlobManager with the connection string and container name.

        Args:
            connection_string (str): The connection string for Azure Blob Storage (optional).
            account_url (str): The URL of the container (optional).
            container_name (str): The name of the container to manage.
        """
        if not connection_string and not account_url:
            raise ValueError("Either connection_string or container_url must be provided.")
        if connection_string and account_url:
            raise ValueError("Only one of connection_string or container_url should be provided.")
        
        self.container_name = container_name

        if account_url:
            self.blob_service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
        else:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)


    @classmethod
    def from_url(cls, account_url: str, container_name: Optional[str] = None) -> 'BlobManager':
        """
        Creates a BlobManager instance from a connection string.
        
        Args:
            account_url (str): The connection string for Azure Blob Storage.
            container_name (str): The name of the container to manage.
        
        Returns:
            BlobManager: An instance of BlobManager.
        """
        return cls(account_url=account_url, container_name=container_name)

    @classmethod
    def from_connection_id(cls, connection_id: str, container_name: Optional[str] = None) -> 'BlobManager':
        """
        Creates a BlobManager instance from an Airflow connection ID.
        
        Args:
            connection_id (str): The Airflow connection ID for Azure Blob Storage.
            container_name (str): The name of the container to manage.
        
        Returns:
            BlobManager: An instance of BlobManager.
        """
        connection: Optional[Connection] = Connection.get(connection_id)
        if not connection:
            raise ValueError(f"Connection with ID {connection_id} not found.")
        connection.extra_dejson
        return cls(connection.get_uri(), container_name)

    def upload_blob(self, blob_name: str, data: bytes, metadata: dict = None):
        """
        Uploads a blob to the specified container.
        
        Args:
            blob_name (str): The name of the blob to upload.
            data (bytes): The data to upload as bytes.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        if metadata:
            blob_client.set_blob_metadata(metadata)
        blob_client.upload_blob(data, overwrite=True)

    def download_blob(self, blob_name: str) -> bytes:
        """
        Downloads a blob from the specified container.
        
        Args:
            blob_name (str): The name of the blob to download.
        
        Returns:
            bytes: The content of the downloaded blob.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
        return blob_client.download_blob().readall()
    