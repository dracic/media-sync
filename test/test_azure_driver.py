"""
Mergin Media Sync - a tool to sync media files from Mergin projects to other storage backends

Copyright (C) 2021 Lutra Consulting

License: MIT
"""

import os
import tempfile
from unittest.mock import MagicMock, patch, mock_open

import pytest

from drivers import AzureDriver, DriverError, DriverType, create_driver
from azure.core.exceptions import AzureError


class MockConfig:
    """Mock config object for Azure driver tests"""

    def __init__(
        self,
        connection_string="DefaultEndpointsProtocol=https;AccountName=test",
        container="test-container",
    ):
        self.driver = "azure"  # Use string for comparison with DriverType enum
        self.azure = MagicMock()
        self.azure.connection_string = connection_string
        self.azure.container = container


@pytest.fixture
def mock_blob_service_client():
    """Fixture to mock BlobServiceClient"""
    with patch("drivers.BlobServiceClient") as mock_client:
        yield mock_client


@pytest.fixture
def mock_container_client(mock_blob_service_client):
    """Fixture to set up mocked container client"""
    mock_container = MagicMock()
    mock_container.exists.return_value = True
    mock_blob_service_client.from_connection_string.return_value.get_container_client.return_value = (
        mock_container
    )
    return mock_container


class TestAzureDriverInit:
    """Tests for AzureDriver initialization"""

    def test_init_with_existing_container(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test initialization when container already exists"""
        mock_container_client.exists.return_value = True

        config = MockConfig()
        driver = AzureDriver(config)

        mock_blob_service_client.from_connection_string.assert_called_once_with(
            config.azure.connection_string
        )
        mock_container_client.exists.assert_called_once()
        mock_container_client.create_container.assert_not_called()

    def test_init_creates_container_if_not_exists(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test initialization creates container when it doesn't exist"""
        mock_container_client.exists.return_value = False

        config = MockConfig()
        driver = AzureDriver(config)

        mock_container_client.create_container.assert_called_once()

    def test_init_raises_driver_error_on_azure_error(self, mock_blob_service_client):
        """Test initialization raises DriverError when Azure SDK fails"""
        mock_blob_service_client.from_connection_string.side_effect = AzureError(
            "Connection failed"
        )

        config = MockConfig()

        with pytest.raises(DriverError, match="Azure driver init error"):
            AzureDriver(config)


class TestAzureDriverUpload:
    """Tests for AzureDriver upload_file method"""

    def test_upload_file_success(
        self, mock_blob_service_client, mock_container_client, tmp_path
    ):
        """Test successful file upload"""
        mock_blob_client = MagicMock()
        mock_blob_client.url = (
            "https://test.blob.core.windows.net/test-container/path/to/file.jpg"
        )
        mock_container_client.get_blob_client.return_value = mock_blob_client

        config = MockConfig()
        driver = AzureDriver(config)

        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"test content")

        result = driver.upload_file(str(test_file), "path/to/file.jpg")

        mock_container_client.get_blob_client.assert_called_once_with(
            "path/to/file.jpg"
        )
        mock_blob_client.upload_blob.assert_called_once()
        assert result == mock_blob_client.url

    def test_upload_file_raises_driver_error_on_azure_error(
        self, mock_blob_service_client, mock_container_client, tmp_path
    ):
        """Test upload_file raises DriverError when Azure SDK fails"""
        mock_blob_client = MagicMock()
        mock_blob_client.upload_blob.side_effect = AzureError("Upload failed")
        mock_container_client.get_blob_client.return_value = mock_blob_client

        config = MockConfig()
        driver = AzureDriver(config)

        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"test content")

        with pytest.raises(DriverError, match="Azure driver error"):
            driver.upload_file(str(test_file), "path/to/file.jpg")


class TestAzureDriverListFiles:
    """Tests for AzureDriver list_files method"""

    def test_list_files_success(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test successful file listing"""
        mock_blobs = [MagicMock(name=f"file{i}.jpg") for i in range(3)]
        for i, blob in enumerate(mock_blobs):
            blob.name = f"file{i}.jpg"
        mock_container_client.list_blobs.return_value = iter(mock_blobs)

        config = MockConfig()
        driver = AzureDriver(config)

        files = list(driver.list_files())

        mock_container_client.list_blobs.assert_called_once_with(name_starts_with=None)
        assert files == ["file0.jpg", "file1.jpg", "file2.jpg"]

    def test_list_files_with_prefix(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test file listing with prefix filter"""
        mock_blobs = [MagicMock(name="images/photo.jpg")]
        mock_blobs[0].name = "images/photo.jpg"
        mock_container_client.list_blobs.return_value = iter(mock_blobs)

        config = MockConfig()
        driver = AzureDriver(config)

        files = list(driver.list_files(prefix="images/"))

        mock_container_client.list_blobs.assert_called_once_with(
            name_starts_with="images/"
        )
        assert files == ["images/photo.jpg"]

    def test_list_files_empty_container(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test listing files from empty container"""
        mock_container_client.list_blobs.return_value = iter([])

        config = MockConfig()
        driver = AzureDriver(config)

        files = list(driver.list_files())

        assert files == []

    def test_list_files_pagination(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test listing files handles pagination (>5000 files) via SDK iterator"""
        mock_blobs = [MagicMock() for i in range(6000)]
        for i, blob in enumerate(mock_blobs):
            blob.name = f"file{i:05d}.jpg"
        mock_container_client.list_blobs.return_value = iter(mock_blobs)

        config = MockConfig()
        driver = AzureDriver(config)

        files = list(driver.list_files())

        assert len(files) == 6000
        assert files[0] == "file00000.jpg"
        assert files[5999] == "file05999.jpg"

    def test_list_files_raises_driver_error_on_azure_error(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test list_files raises DriverError when Azure SDK fails"""
        mock_container_client.list_blobs.side_effect = AzureError("List failed")

        config = MockConfig()
        driver = AzureDriver(config)

        with pytest.raises(DriverError, match="Azure driver list error"):
            list(driver.list_files())


class TestAzureDriverGetFileUrl:
    """Tests for AzureDriver get_file_url method"""

    def test_get_file_url_returns_blob_url(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test get_file_url returns the correct blob URL"""
        mock_blob_client = MagicMock()
        mock_blob_client.url = (
            "https://test.blob.core.windows.net/test-container/path/to/file.jpg"
        )
        mock_container_client.get_blob_client.return_value = mock_blob_client

        config = MockConfig()
        driver = AzureDriver(config)

        url = driver.get_file_url("path/to/file.jpg")

        mock_container_client.get_blob_client.assert_called_once_with("path/to/file.jpg")
        assert url == "https://test.blob.core.windows.net/test-container/path/to/file.jpg"


class TestCreateDriverAzure:
    """Tests for create_driver factory with Azure"""

    def test_create_driver_returns_azure_driver(
        self, mock_blob_service_client, mock_container_client
    ):
        """Test create_driver returns AzureDriver for azure driver type"""
        config = MockConfig()
        driver = create_driver(config)

        assert isinstance(driver, AzureDriver)
