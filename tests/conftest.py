# conftest.py - Configuration for pytest
# This file can be used to define fixtures, hooks, and plugins
# shared across multiple test files.

import pytest
import pytest_asyncio  # Import this to use async fixtures
import asyncio
from db import DatabaseManager # Assuming db.py is in the root and accessible
from unittest.mock import MagicMock, patch

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
    """Provides an IMAPClient instance with imaplib.IMAP4_SSL mocked."""
    # Mock credentials - for ImapClient, we assume creds are valid when passed in.
    mock_creds = MagicMock()
    mock_creds.token = "test_oauth_token"

    with patch('imap_client.imaplib.IMAP4_SSL') as mock_imap_constructor:
        # Configure the mock IMAP4_SSL instance that will be returned by the constructor
        mock_imap_instance = MagicMock()
        mock_imap_constructor.return_value = mock_imap_instance
        
        # Create an instance of your ImapClient
        # We import ImapClient here to ensure it uses the patched imaplib
        from imap_client import ImapClient 
        client = ImapClient(host="test.imap.server", user="testuser@example.com", creds=mock_creds)
        client.imap = mock_imap_instance # Directly assign the mock instance for easier control in tests
                                        # The connect() method would normally do this via _login_oauth2
                                        # For many tests, we might want to bypass full connect()

        yield client, mock_imap_instance # Yield both client and the direct mock for assertions

        # No explicit cleanup needed for the client here as it doesn't hold async resources
        # that the fixture itself opened (like db_manager did). 
        # The ImapClient.close() would be tested separately. 