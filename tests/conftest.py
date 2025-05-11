# conftest.py - Configuration for pytest
# This file can be used to define fixtures, hooks, and plugins
# shared across multiple test files.

import pytest
import pytest_asyncio  # Import this to use async fixtures
import asyncio
from db import DatabaseManager # Assuming db.py is in the root and accessible
from unittest.mock import MagicMock, patch, AsyncMock

# Added imports
from imap_client import ImapClient
from checkpoint import CheckpointManager as ActualCheckpointManager
import datetime as dtmodule # Alias to avoid conflict with fixture names

@pytest_asyncio.fixture
async def db_manager():
    """
    Provides a DatabaseManager instance connected to an in-memory SQLite database
    with schema initialized.
    """
    # Use :memory: for an in-memory database for tests
    manager = DatabaseManager(":memory:")
    await manager.connect()  # connect also calls setup_schema
    yield manager  # Yield the manager instance
    await manager.close()

@pytest_asyncio.fixture
async def mock_imap_client():
    """Provides an ImapClient instance with imaplib.IMAP4_SSL mocked."""
    # Mock credentials - for ImapClient, we assume creds are valid when passed in.
    mock_creds = MagicMock()
    mock_creds.token = "test_oauth_token"

    with patch('imap_client.imaplib.IMAP4_SSL') as mock_imap_constructor:
        # Configure the mock IMAP4_SSL instance that will be returned by the constructor
        mock_imap_instance = MagicMock()
        mock_imap_constructor.return_value = mock_imap_instance
        
        # Create an instance of your ImapClient
        # We import ImapClient here to ensure it uses the patched imaplib
        # This local import can be removed if ImapClient is imported at the top (which it is now)
        # from imap_client import ImapClient 
        client = ImapClient(host="test.imap.server", user="testuser@example.com", creds=mock_creds)
        client.imap = mock_imap_instance # Directly assign the mock instance for easier control in tests
                                        # The connect() method would normally do this via _login_oauth2
                                        # For many tests, we might want to bypass full connect()

        yield client, mock_imap_instance # Yield both client and the direct mock for assertions

        # No explicit cleanup needed for the client here as it doesn't hold async resources
        # that the fixture itself opened (like db_manager did). 
        # The ImapClient.close() would be tested separately. 

# New Fixtures Start Here

@pytest.fixture
def mocked_db_interface():
    """Provides a MagicMock for the DatabaseManager, mocking its interface."""
    mock = MagicMock(spec=DatabaseManager)
    mock.log_sync_start = AsyncMock(return_value=1)
    mock.log_sync_end = AsyncMock()
    mock.save_email_header = AsyncMock()
    mock.save_full_email = AsyncMock()
    mock.get_max_synced_header_uid = AsyncMock(return_value=0)
    mock.get_uids_for_full_sync = AsyncMock(return_value=[])
    mock.commit_with_retry = AsyncMock()
    # For CheckpointManager dependency
    mock.load_checkpoint_state = AsyncMock(return_value=None)
    mock.save_checkpoint_state = AsyncMock()
    mock.add_or_update_checkpoint_failed_uid = AsyncMock()
    mock.remove_checkpoint_failed_uid = AsyncMock()
    mock.get_checkpoint_failed_uids = AsyncMock(return_value={})

    # Add a mock for the .db attribute, as it's accessed in main.py handlers
    mock.db = MagicMock() # Or AsyncMock() if its methods are awaited

    return mock

@pytest.fixture
def mocked_imap_client_interface():
    """Provides a MagicMock for the ImapClient, mocking its interface."""
    mock = MagicMock(spec=ImapClient)
    mock.connect = AsyncMock()
    mock.close = AsyncMock()
    mock.select_mailbox = AsyncMock(return_value=True)
    mock.fetch = AsyncMock(return_value=('OK', [(b'HEADER', b'mock_header_data')]))
    mock.search_all_uids_since = AsyncMock(return_value=[])
    mock.search_by_date_chunks = AsyncMock(return_value=[])
    mock.search_all = AsyncMock(return_value=[])
    mock.list_mailboxes = AsyncMock(return_value=[])
    return mock

@pytest_asyncio.fixture
async def patched_checkpoint_manager():
    """Patches 'sync.CheckpointManager' and provides a pre-configured AsyncMock instance."""
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.last_uid = 0
        mock_cm_instance.failed_uids = {}
        mock_cm_instance.in_progress = False
        mock_cm_instance.timestamp = None
        mock_cm_instance.mark_start = AsyncMock()
        mock_cm_instance.mark_complete = AsyncMock()
        mock_cm_instance.update_progress = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock()
        mock_cm_instance.clear_failed_uid = AsyncMock()
        mock_cm_instance.save_state = AsyncMock()
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_uids_to_retry = AsyncMock(return_value=[])
        mock_cm_instance.get_permanently_failed_uids = AsyncMock(return_value=[])
        mock_cm_instance.was_interrupted = AsyncMock(return_value=False)
        PatchedCheckpointManager.return_value = mock_cm_instance
        yield mock_cm_instance

@pytest.fixture
def mock_sync_datetime():
    """Patches 'sync.datetime' module."""
    with patch('sync.datetime') as mock_datetime_module:
        # Common default for now()
        mock_datetime_module.datetime.now.return_value = dtmodule.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dtmodule.timezone.utc)
        yield mock_datetime_module

@pytest.fixture
def mock_checkpoint_datetime():
    """Patches 'checkpoint.datetime' module."""
    with patch('checkpoint.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = dtmodule.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dtmodule.timezone.utc)
        yield mock_datetime_module

@pytest.fixture
def mock_main_datetime():
    """Patches 'main.datetime' module."""
    with patch('main.datetime') as mock_datetime_module:
        mock_datetime_module.datetime.now.return_value = dtmodule.datetime(2023, 1, 1, 12, 0, 0, tzinfo=dtmodule.timezone.utc)
        # If main.py uses date.today() or other datetime components, mock them here too if needed
        yield mock_datetime_module 