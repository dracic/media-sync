"""
Mergin Media Sync - a tool to sync media files from Mergin projects to other storage backends

Copyright (C) 2021 Lutra Consulting

License: MIT
"""

import os
from pathlib import Path
import shutil
import typing
import re
import enum

from minio import Minio
from minio.error import S3Error
from urllib.parse import urlparse, urlunparse

from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.http import MediaFileUpload

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError


class DriverType(enum.Enum):
    LOCAL = "local"
    MINIO = "minio"
    GOOGLE_DRIVE = "google_drive"
    AZURE = "azure"

    def __eq__(self, value):
        if isinstance(value, str):
            return self.value == value

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class DriverError(Exception):
    pass


class Driver:
    def __init__(self, config):
        self.config = config

    def upload_file(self, src, obj_path):
        """Copy object to destination and return path"""
        raise NotImplementedError

    def list_files(self, prefix: str = None) -> typing.Generator[str, None, None]:
        """List all file names in the storage.

        Args:
            prefix: Optional prefix to filter files by path/name.

        Yields:
            File paths/names as strings.
        """
        raise NotImplementedError

    def get_file_url(self, obj_path: str) -> str:
        """Get the URL/path for an existing file without uploading.

        Used to retrieve destination paths for files that already exist,
        enabling reference updates during resume/deduplication scenarios.

        Args:
            obj_path: The object path/name in the storage.

        Returns:
            The full URL or path to the file in the destination storage.
        """
        raise NotImplementedError

    def file_exists(self, obj_path: str) -> bool:
        """Check if a file exists in the storage without listing all files.

        This is more efficient than list_files() when checking a small number
        of files against a large remote storage.

        Args:
            obj_path: The object path/name in the storage.

        Returns:
            True if the file exists, False otherwise.
        """
        raise NotImplementedError


class LocalDriver(Driver):
    """Driver to work with local drive, for testing purpose mainly"""

    def __init__(self, config):
        super(LocalDriver, self).__init__(config)
        self.dest = config.local.dest

        try:
            if not os.path.exists(self.dest):
                os.makedirs(self.dest)
        except OSError as e:
            raise DriverError("Local driver init error: " + str(e))

    def file_exists(self, obj_path: str) -> bool:
        """Check if a file exists in the local destination."""
        return os.path.exists(os.path.join(self.dest, obj_path))

    def upload_file(self, src, obj_path):
        dest = os.path.join(self.dest, obj_path)
        dest_dir = os.path.dirname(dest)
        try:
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            shutil.copyfile(src, dest)
        except (shutil.SameFileError, OSError) as e:
            raise DriverError("Local driver error: " + str(e))
        return dest

    def list_files(self, prefix: str = None) -> typing.Generator[str, None, None]:
        """List all files in the local destination directory.

        Args:
            prefix: Optional prefix to filter files by path.

        Yields:
            Relative file paths as strings.
        """
        try:
            for root, dirs, files in os.walk(self.dest):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, self.dest)
                    rel_path = rel_path.replace(os.sep, "/")
                    if prefix is None or rel_path.startswith(prefix):
                        yield rel_path
        except OSError as e:
            raise DriverError("Local driver list error: " + str(e))

    def get_file_url(self, obj_path: str) -> str:
        """Get the full path for an existing file."""
        return os.path.join(self.dest, obj_path)


class MinioDriver(Driver):
    """Driver to handle connection to minio-like server"""

    def __init__(self, config):
        super(MinioDriver, self).__init__(config)

        try:
            self.client = Minio(
                endpoint=config.minio.endpoint,
                access_key=config.minio.access_key,
                secret_key=config.minio.secret_key,
                secure=config.as_bool("minio.secure"),
                region=config.minio.region,
            )
            self.bucket = config.minio.bucket
            bucket_found = self.client.bucket_exists(self.bucket)
            if not bucket_found:
                self.client.make_bucket(self.bucket)

            self.bucket_subpath = None
            if hasattr(config.minio, "bucket_subpath"):
                if config.minio.bucket_subpath:
                    # Normalize: strip leading/trailing slashes to prevent double-slash paths
                    self.bucket_subpath = config.minio.bucket_subpath.strip("/")

            # construct base url for bucket
            scheme = "https://" if config.as_bool("minio.secure") else "http://"

            if config.minio.region and "amazonaws" in config.minio.endpoint.lower():
                self.base_url = (
                    f"{scheme}{self.bucket}.s3.{config.minio.region}.amazonaws.com"
                )
            else:
                self.base_url = scheme + config.minio.endpoint + "/" + self.bucket
        except S3Error as e:
            raise DriverError("MinIO driver init error: " + str(e))

    def upload_file(self, src, obj_path):
        if self.bucket_subpath:
            obj_path = f"{self.bucket_subpath}/{obj_path}"
        try:
            res = self.client.fput_object(self.bucket, obj_path, src)
            dest = self.base_url + "/" + res.object_name
        except S3Error as e:
            raise DriverError("MinIO driver error: " + str(e))
        return dest

    def list_files(self, prefix: str = None) -> typing.Generator[str, None, None]:
        """List all object names in the bucket.

        Args:
            prefix: Optional prefix to filter objects by name.

        Yields:
            Object names as strings.
        """
        try:
            effective_prefix = prefix
            if self.bucket_subpath:
                # Ensure subpath is treated as a directory (trailing slash)
                # to avoid matching siblings like "data" matching "database.csv"
                subpath_dir = self.bucket_subpath if self.bucket_subpath.endswith("/") else f"{self.bucket_subpath}/"
                if prefix:
                    effective_prefix = f"{subpath_dir}{prefix}"
                else:
                    effective_prefix = subpath_dir
            objects = self.client.list_objects(
                self.bucket, prefix=effective_prefix, recursive=True
            )
            for obj in objects:
                # Skip directory markers (some S3-compatible storages return these)
                if obj.is_dir:
                    continue
                name = obj.object_name
                prefix_to_strip = f"{self.bucket_subpath}/" if self.bucket_subpath else ""
                if prefix_to_strip and name.startswith(prefix_to_strip):
                    name = name[len(prefix_to_strip):]
                yield name
        except S3Error as e:
            raise DriverError("MinIO driver list error: " + str(e))

    def get_file_url(self, obj_path: str) -> str:
        """Get the URL for an existing file in the bucket."""
        if self.bucket_subpath:
            obj_path = f"{self.bucket_subpath}/{obj_path}"
        return self.base_url + "/" + obj_path

    def file_exists(self, obj_path: str) -> bool:
        """Check if an object exists in the bucket using stat_object (HEAD request)."""
        if self.bucket_subpath:
            obj_path = f"{self.bucket_subpath}/{obj_path}"
        try:
            self.client.stat_object(self.bucket, obj_path)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise DriverError("MinIO driver exists error: " + str(e))


class GoogleDriveDriver(Driver):
    """Driver to handle connection to Google Drive"""

    def __init__(self, config):
        super(GoogleDriveDriver, self).__init__(config)

        try:
            self._credentials = service_account.Credentials.from_service_account_file(
                Path(config.google_drive.service_account_file),
                scopes=["https://www.googleapis.com/auth/drive.file"],
            )

            self._service: Resource = build(
                "drive", "v3", credentials=self._credentials
            )

            self._folder = config.google_drive.folder
            self._folder_id = self._folder_exists(self._folder)

            if not self._folder_id:
                self._folder_id = self._create_folder(self._folder)

            for email in self._get_share_with(config.google_drive):
                if email:
                    self._share_with(email)

        except Exception as e:
            raise DriverError("GoogleDrive driver init error: " + str(e))

    def upload_file(self, src: str, obj_path: str) -> str:
        try:
            file_metadata = {
                "name": obj_path,
                "parents": [self._folder_id],
            }
            media = MediaFileUpload(src)

            file = (
                self._service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )

            file_id = file.get("id")

        except Exception as e:
            raise DriverError("GoogleDrive driver error: " + str(e))

        return self._file_link(file_id)

    def _folder_exists(self, folder_name: str) -> typing.Optional[str]:
        """Check if a folder with the specified name exists. Return boolean and folder ID if exists."""

        # Query to check if a folder with the specified name exists
        try:
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            results = (
                self._service.files().list(q=query, fields="files(id, name)").execute()
            )
            items = results.get("files", [])
        except Exception as e:
            raise DriverError("Google Drive folder exists error: " + str(e))

        if len(items) > 1:
            print(
                f"Multiple folders with name '{folder_name}' found. Using the first one found."
            )

        if items:
            return items[0]["id"]
        else:
            return None

    def _create_folder(self, folder_name: str) -> str:
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }

        try:
            folder = (
                self._service.files().create(body=file_metadata, fields="id").execute()
            )
            return folder.get("id")
        except Exception as e:
            raise DriverError("Google Drive create folder error: " + str(e))

    def _file_link(self, file_id: str) -> str:
        """Get a link to the file in Google Drive."""
        try:
            file = (
                self._service.files()
                .get(fileId=file_id, fields="webViewLink")
                .execute()
            )
            return file.get("webViewLink")
        except Exception as e:
            raise DriverError("Google Drive file link error: " + str(e))

    def _has_already_permission(self, email: str) -> bool:
        """Check if email already has permission to the folder."""
        try:
            # List all permissions for the file
            permissions = (
                self._service.permissions()
                .list(
                    fileId=self._folder_id,
                    fields="permissions(id, emailAddress, role, type)",
                )
                .execute()
            )

            return any(
                permission.get("emailAddress", "").lower() == email.lower()
                for permission in permissions.get("permissions", [])
            )

        except Exception as e:
            raise DriverError("Google Drive has permission error: " + str(e))

        return False

    def _share_with(self, email: str) -> None:
        """Share the folder with the specified email."""
        if not self._has_already_permission(email):
            try:
                permission = {
                    "type": "user",
                    "role": "writer",
                    "emailAddress": email,
                }
                self._service.permissions().create(
                    fileId=self._folder_id, body=permission
                ).execute()
            except Exception as e:
                raise DriverError("Google Drive sharing folder error: " + str(e))

    def _get_share_with(self, config_google_drive) -> typing.List[str]:
        email_regex = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")

        emails_to_share_with = []
        if isinstance(config_google_drive.share_with, str):
            if email_regex.match(config_google_drive.share_with):
                emails_to_share_with.append(config_google_drive.share_with)
        elif isinstance(config_google_drive.share_with, list):
            for email in config_google_drive.share_with:
                if email_regex.match(email):
                    emails_to_share_with.append(email)
        else:
            raise DriverError(
                "Google Drive sharing: Incorrect GoogleDrive shared_with settings"
            )

        if not emails_to_share_with:
            print("Google Drive sharing: Not shared with any user")

        return emails_to_share_with

    def list_files(self, prefix: str = None) -> typing.Generator[str, None, None]:
        """List all files in the Google Drive folder.

        Uses pagination to handle folders with many files.
        Pre-fetches webViewLink URLs to avoid N+1 API calls during skip_existing.

        Note: Prefix filtering is performed client-side using Python's startswith()
        because Google Drive's API 'contains' operator doesn't support startswith
        semantics. For folders with many files (>100,000), this may be slow as all
        file metadata must be transferred over the network before filtering.

        Args:
            prefix: Optional prefix to filter files by name.

        Yields:
            File names as strings.
        """
        try:
            # Initialize URL cache for get_file_url optimization
            if not hasattr(self, "_url_cache"):
                self._url_cache = {}

            # Exclude folders from listing
            query = (
                f"'{self._folder_id}' in parents and trashed = false "
                f"and mimeType != 'application/vnd.google-apps.folder'"
            )

            page_token = None
            while True:
                results = (
                    self._service.files()
                    .list(
                        q=query,
                        fields="nextPageToken, files(name, webViewLink)",
                        pageToken=page_token,
                        pageSize=1000,
                    )
                    .execute()
                )

                for file in results.get("files", []):
                    name = file.get("name")
                    # Filter by prefix client-side for accurate startswith matching
                    if prefix is None or name.startswith(prefix):
                        # Only cache URLs for files we actually yield to prevent
                        # unbounded memory growth on large folders
                        self._url_cache[name] = file.get("webViewLink", "")
                        yield name

                page_token = results.get("nextPageToken")
                if not page_token:
                    break
        except Exception as e:
            raise DriverError("Google Drive list error: " + str(e))

    def get_file_url(self, obj_path: str) -> str:
        """Get the webViewLink for an existing file in Google Drive.

        Uses cached URLs from list_files() when available to avoid N+1 API calls.
        Falls back to API lookup if not in cache.
        """
        # Check cache first (populated by list_files)
        if hasattr(self, "_url_cache") and obj_path in self._url_cache:
            return self._url_cache[obj_path]

        try:
            # Fallback: API lookup if not in cache
            # Escape backslashes first, then single quotes for Google Drive query
            escaped_name = obj_path.replace("\\", "\\\\").replace("'", "\\'")
            query = (
                f"'{self._folder_id}' in parents and "
                f"name = '{escaped_name}' and trashed = false"
            )
            results = (
                self._service.files()
                .list(q=query, fields="files(id, webViewLink)", pageSize=1)
                .execute()
            )
            files = results.get("files", [])
            if files:
                return files[0].get("webViewLink", "")
            return ""
        except Exception as e:
            raise DriverError("Google Drive get file URL error: " + str(e))

    def file_exists(self, obj_path: str) -> bool:
        """Check if a file exists in the Google Drive folder."""
        # Check cache first (populated by list_files)
        if hasattr(self, "_url_cache") and obj_path in self._url_cache:
            return True

        try:
            # Query for exact file name match
            escaped_name = obj_path.replace("\\", "\\\\").replace("'", "\\'")
            query = (
                f"'{self._folder_id}' in parents and "
                f"name = '{escaped_name}' and trashed = false"
            )
            results = (
                self._service.files()
                .list(q=query, fields="files(id)", pageSize=1)
                .execute()
            )
            return len(results.get("files", [])) > 0
        except Exception as e:
            raise DriverError("Google Drive file exists error: " + str(e))


class AzureDriver(Driver):
    """Driver to handle connection to Azure Blob Storage"""

    def __init__(self, config):
        super(AzureDriver, self).__init__(config)

        try:
            self._connection_string = config.azure.connection_string
            self._container_name = config.azure.container

            self._blob_service_client = BlobServiceClient.from_connection_string(
                self._connection_string
            )
            self._container_client = self._blob_service_client.get_container_client(
                self._container_name
            )

            if not self._container_client.exists():
                self._container_client.create_container()

        except AzureError as e:
            raise DriverError("Azure driver init error: " + str(e))

    def upload_file(self, src: str, obj_path: str) -> str:
        try:
            blob_client = self._container_client.get_blob_client(obj_path)
            with open(src, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            return blob_client.url
        except AzureError as e:
            raise DriverError("Azure driver error: " + str(e))

    def list_files(self, prefix: str = None) -> typing.Generator[str, None, None]:
        """List all blob names in the container.

        The Azure SDK's ItemPaged iterator automatically handles
        pagination/continuation tokens for >5000 files.

        Args:
            prefix: Optional prefix to filter blobs by name.

        Yields:
            Blob names as strings.
        """
        try:
            blobs = self._container_client.list_blobs(name_starts_with=prefix)
            for blob in blobs:
                yield blob.name
        except AzureError as e:
            raise DriverError("Azure driver list error: " + str(e))

    def get_file_url(self, obj_path: str) -> str:
        """Get the URL for an existing blob in the container."""
        blob_client = self._container_client.get_blob_client(obj_path)
        return blob_client.url

    def file_exists(self, obj_path: str) -> bool:
        """Check if a blob exists in the container."""
        try:
            blob_client = self._container_client.get_blob_client(obj_path)
            return blob_client.exists()
        except AzureError as e:
            raise DriverError("Azure driver exists error: " + str(e))


def create_driver(config):
    """Create driver object based on type defined in config"""
    driver = None
    if config.driver == DriverType.LOCAL:
        driver = LocalDriver(config)
    elif config.driver == DriverType.MINIO:
        driver = MinioDriver(config)
    elif config.driver == DriverType.GOOGLE_DRIVE:
        driver = GoogleDriveDriver(config)
    elif config.driver == DriverType.AZURE:
        driver = AzureDriver(config)
    return driver
