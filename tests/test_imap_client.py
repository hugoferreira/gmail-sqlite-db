import pytest
from unittest.mock import patch, MagicMock, ANY
import imaplib
from imap_client import ImapClient
import datetime
import unittest

# Placeholder for now, we'll add tests after reviewing imap_client.py

@pytest.mark.skip(reason="Not yet implemented")
def test_example_imap_client_placeholder():
    assert True

# --- Tests for ImapClient ---

# Need to import ImapClient here for type hinting and direct instantiation if not using the fixture for all tests
from imap_client import ImapClient, imaplib # Import imaplib for IMAP4.error

@pytest.mark.asyncio
async def test_connect_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    
    # Mock the _login_oauth2 to simulate a successful login which sets self.imap
    # The fixture already sets client.imap, but connect() calls _login_oauth2
    # So we need to ensure _login_oauth2 (when called by connect) returns the mock_imap_instance
    # or that the authenticate call on the instance succeeds.
    
    # Scenario 1: If _login_oauth2 is called by connect and we want to mock its internal imaplib.authenticate
    # The mock_imap_instance is what's constructed by _login_oauth2's IMAP4_SSL call.
    mock_imap_instance.authenticate.return_value = ('OK', [b'Authenticated'])
    
    # We need to reset client.imap to None so connect() tries to establish it.
    # The fixture pre-sets it for convenience, but for testing connect(), we undo that.
    client.imap = None

    # We also need to ensure that when client.loop.run_in_executor is called with _login_oauth2,
    # it executes something that involves the mock_imap_instance.authenticate.
    # The patch in the fixture handles the IMAP4_SSL constructor.
    # _login_oauth2 will use the mock_imap_instance created by the patched constructor.

    # Don't actually print during tests unless debugging
    with patch('builtins.print') as mock_print:
        connected_imap = await client.connect()
    
    assert connected_imap is mock_imap_instance
    assert client.imap is mock_imap_instance
    mock_imap_instance.authenticate.assert_called_once_with(
        'XOAUTH2',
        ANY
    )
    # Check for print statements
    mock_print.assert_any_call(f"Connecting to {client.host} as {client.user}...")
    mock_print.assert_any_call(f"Connection established successfully")

@pytest.mark.asyncio
async def test_connect_auth_failure(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    
    # Simulate authentication failure
    # The imaplib.IMAP4.error needs to be raised by the authenticate call
    # This imaplib needs to be the one from the imap_client module's scope if it re-imports
    mock_imap_instance.authenticate.side_effect = imaplib.IMAP4.error("AUTHENTICATIONFAILED Invalid credentials")
    
    client.imap = None # Ensure connect tries to login

    with patch('builtins.print') as mock_print:
        with pytest.raises(ConnectionRefusedError, match="OAuth2 authentication failed. Check token."):
            await client.connect()
            
    assert client.imap is None # Should not be set on failure
    assert mock_imap_instance.authenticate.call_count >= 1 # Allow for retries
    mock_print.assert_any_call("OAuth2 authentication failed. Token might be invalid or expired.")


# Test the synchronous helper method directly
@pytest.mark.parametrize("mailbox_name, expected", [
    ("INBOX", "INBOX"),
    ("Sent Items", '"Sent Items"'),
    ("Work/Projects", '"Work/Projects"'),
    ('"Already Quoted"', '"Already Quoted"'),
    ("Archive/Old", '"Archive/Old"'),
])
def test_quote_mailbox_if_needed(mailbox_name, expected):
    # We need an ImapClient instance just to call this method, 
    # it doesn't rely on connection state.
    # Provide minimal mock creds for instantiation.
    mock_creds = MagicMock()
    mock_creds.token = "dummy"
    client = ImapClient(host="anyhost", user="anyuser", creds=mock_creds)
    assert client._quote_mailbox_if_needed(mailbox_name) == expected

@pytest.mark.asyncio
async def test_select_mailbox_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    mailbox_name = "INBOX"
    mock_imap_instance.select.return_value = ('OK', [b'Mailbox selected'])

    with patch('builtins.print') as mock_print: # if select_mailbox logs
        result = await client.select_mailbox(mailbox_name)
    
    assert result is True
    assert client.current_mailbox == mailbox_name
    mock_imap_instance.select.assert_called_once_with(client._quote_mailbox_if_needed(mailbox_name), readonly=True)

@pytest.mark.asyncio
async def test_select_mailbox_already_selected(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    mailbox_name = "INBOX"
    client.current_mailbox = mailbox_name # Simulate it's already selected

    result = await client.select_mailbox(mailbox_name)
    
    assert result is True
    mock_imap_instance.select.assert_not_called() # Should not call select on the server

@pytest.mark.asyncio
async def test_select_mailbox_failure(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    mailbox_name = "NonExistentMailbox"
    mock_imap_instance.select.return_value = ('NO', [b'Error selecting mailbox'])

    with patch('builtins.print') as mock_print:
        result = await client.select_mailbox(mailbox_name)
    
    assert result is False
    assert client.current_mailbox is None # Or remains what it was, depending on desired behavior
    mock_imap_instance.select.assert_called_once_with(client._quote_mailbox_if_needed(mailbox_name), readonly=True)
    mock_print.assert_any_call(f"Failed to select mailbox {mailbox_name}: NO")

@pytest.mark.asyncio
async def test_list_mailboxes_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    # Example from imaplib documentation for LIST response
    # mailboxes_data = [
    #    b'(\\HasNoChildren) "/" INBOX',
    #    b'(\\Noselect \\HasChildren) "/" "[Gmail]"',
    #    b'(\\HasNoChildren \\Sent) "/" "[Gmail]/Sent Mail"'
    # ]
    # The client code parses these.
    raw_mailbox_data = [
        b'(\\HasNoChildren) "/" INBOX',
        b'(\\HasNoChildren) "/" Sent',
        b'(\\HasChildren \\Noselect) "/" "[Gmail]"',
        b'(\\HasNoChildren) "/" "[Gmail]/Drafts"',
        b'(\\HasNoChildren) "/" "Archive/2023"', # Test quoting requirement for name
    ]
    mock_imap_instance.list.return_value = ('OK', raw_mailbox_data)

    # Adjusted expected_mailboxes to include "[Gmail]" as it's a validly parsed name
    expected_mailboxes = ["INBOX", "Sent", "[Gmail]", "[Gmail]/Drafts", "Archive/2023"]
    
    mailboxes = await client.list_mailboxes()

    assert mailboxes == expected_mailboxes
    mock_imap_instance.list.assert_called_once()

@pytest.mark.asyncio
async def test_list_mailboxes_failure(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    mock_imap_instance.list.return_value = ('NO', [b'Error listing mailboxes'])

    mailboxes = await client.list_mailboxes()

    assert mailboxes == []
    mock_imap_instance.list.assert_called_once()

@pytest.mark.asyncio
async def test_list_mailboxes_decoding_and_parsing(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    # Malformed or tricky entries
    raw_mailbox_data = [
        b'(\\HasNoChildren) "/" "Normal Name"',
        b'() "/" "UTF8\xc3\xbcName"', # UTF-8 name (üName)
        b'() " " "Spaced / Slashed"', # Name client._quote_mailbox_if_needed would quote
        b'NIL', # Should be ignored by typical parsing logic in client
        b'(\\Noselect) "/" "[Google Mail]"' # Another common form
    ]
    mock_imap_instance.list.return_value = ('OK', raw_mailbox_data)

    # The client code splits by ' "' and takes the last part, then rstrip('"')
    # For "UTF8\xc3\xbcName", it will become "UTF8üName"
    # For "Spaced / Slashed", it will become "Spaced / Slashed"
    expected_mailboxes = ["Normal Name", "UTF8üName", "Spaced / Slashed", "[Google Mail]"]
    mailboxes = await client.list_mailboxes()
    assert mailboxes == expected_mailboxes

@pytest.mark.asyncio
async def test_search_all_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    # Assume a mailbox is selected
    client.current_mailbox = "INBOX"
    mock_imap_instance.uid.return_value = ('OK', [b'1 2 3 4 5'])

    uids = await client.search_all()
    assert uids == ['1', '2', '3', '4', '5']
    mock_imap_instance.uid.assert_called_once_with('SEARCH', None, 'ALL')

@pytest.mark.asyncio
async def test_search_all_no_messages(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "INBOX"
    mock_imap_instance.uid.return_value = ('OK', [b''])

    uids = await client.search_all()
    assert uids == [] # Or [''] if that's how split behaves, client code filters this

@pytest.mark.asyncio
async def test_search_all_failure(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "INBOX"
    mock_imap_instance.uid.return_value = ('NO', [b'Search failed'])

    uids = await client.search_all()
    assert uids == []

# --- Tests for search_chunked ---
@pytest.mark.asyncio
async def test_search_chunked_small_mailbox(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "INBOX"

    # Simulate status call for message count
    mock_imap_instance.status.return_value = ('OK', [b'(MESSAGES 50)'])
    # Simulate the fallback search_all call
    mock_imap_instance.uid.return_value = ('OK', [b'10 20 30']) # UIDs from search_all

    with patch('builtins.print') as mock_print:
        uids = await client.search_chunked(chunk_size=100)
    
    assert uids == ['10', '20', '30']
    mock_imap_instance.status.assert_called_once_with("INBOX", '(MESSAGES)')
    mock_imap_instance.uid.assert_called_once_with('SEARCH', None, 'ALL') # search_all was used
    mock_imap_instance.fetch.assert_not_called() # UID fetch by sequence set not used

@pytest.mark.asyncio
async def test_search_chunked_large_mailbox(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "INBOX"
    chunk_s = 2 # Small chunk size for testing

    # Simulate status call for message count
    mock_imap_instance.status.return_value = ('OK', [b'(MESSAGES 3)'])
    
    # Simulate fetch calls for UID chunks
    # Chunk 1: messages 1-2
    # response format: [(b'1 (UID 101)', b')'), (b'2 (UID 102)', b')')] (simplified)
    # The client code parses this format: re.search(r'UID\s+(\d+)', item[0].decode())
    fetch_chunk1_response = [
        (b'1 (UID 101)', b'stuff'), 
        (b'2 (UID 102)', b'stuff')
    ]
    # Chunk 2: message 3
    fetch_chunk2_response = [
        (b'3 (UID 103)', b'stuff')
    ]
    
    mock_imap_instance.fetch.side_effect = [
        ('OK', fetch_chunk1_response),
        ('OK', fetch_chunk2_response)
    ]

    with patch('builtins.print') as mock_print:
        uids = await client.search_chunked(chunk_size=chunk_s)

    assert uids == ['101', '102', '103']
    mock_imap_instance.status.assert_called_once_with("INBOX", '(MESSAGES)')
    assert mock_imap_instance.fetch.call_count == 2
    mock_imap_instance.fetch.assert_any_call("1:2", '(UID)')
    mock_imap_instance.fetch.assert_any_call("3:3", '(UID)')
    mock_print.assert_any_call(f"Large mailbox detected (3 messages). Using chunked fetch.")

@pytest.mark.asyncio
async def test_search_chunked_status_fails(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "INBOX"
    mock_imap_instance.status.return_value = ('NO', [b'Cannot get status'])
    # Fallback to search_all
    mock_imap_instance.uid.return_value = ('OK', [b'90 91'])

    with patch('builtins.print') as mock_print:
        uids = await client.search_chunked(chunk_size=100)
    
    assert uids == ['90', '91']
    mock_print.assert_any_call(f"Warning: Could not get message count for INBOX")
    mock_imap_instance.uid.assert_called_once_with('SEARCH', None, 'ALL')

# --- Tests for search_by_date_chunks ---
@pytest.mark.asyncio
async def test_search_by_date_chunks_basic(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "ARCHIVE"
    start_year = 2023
    # Let's mock datetime.now() to control the end month for the loop
    # So it doesn't try to fetch for future months in the current year if test runs late in the year.
    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls):
            return cls(2023, 2, 15) # Mock current date to Feb 2023

    with patch('imap_client.datetime.datetime', MockDateTime):
        # Simulate uid SEARCH calls by date
        # Jan 2023
        search_jan_response = ('OK', [b'201 202'])
        # Feb 2023
        search_feb_response = ('OK', [b'203'])
        
        mock_imap_instance.uid.side_effect = [
            search_jan_response, 
            search_feb_response
            # Any further calls for Mar-Dec 2023 for year=2023 would not happen due to mocked now()
        ]

        with patch('builtins.print') as mock_print:
            uids = await client.search_by_date_chunks(start_year=start_year, end_year=start_year)
    
    assert uids == ['201', '202', '203']
    assert mock_imap_instance.uid.call_count == 2 # Jan, Feb
    # Check one of the calls
    mock_imap_instance.uid.assert_any_call('SEARCH', None, '(SINCE "01-JAN-2023" BEFORE "31-JAN-2023")')
    mock_imap_instance.uid.assert_any_call('SEARCH', None, '(SINCE "01-FEB-2023" BEFORE "28-FEB-2023")')
    mock_print.assert_any_call("  Found 2 messages for Jan 2023")
    mock_print.assert_any_call("  Found 1 messages for Feb 2023")

@pytest.mark.asyncio
async def test_search_by_date_chunks_search_fails(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.current_mailbox = "ARCHIVE"
    start_year = 2023
    class MockDateTime(datetime.datetime):
        @classmethod
        def now(cls):
            return cls(2023, 1, 15) # Mock current date to Jan 2023

    with patch('imap_client.datetime.datetime', MockDateTime):
        mock_imap_instance.uid.return_value = ('NO', [b'Search error for date range'])
        with patch('builtins.print') as mock_print:
            uids = await client.search_by_date_chunks(start_year=start_year, end_year=start_year)

    assert uids == []
    # It will only try for Jan 2023 due to mocked date
    mock_imap_instance.uid.assert_called_once_with('SEARCH', None, '(SINCE "01-JAN-2023" BEFORE "31-JAN-2023")')
    mock_print.assert_any_call(f"Warning: Failed to search date range 01-JAN-2023 to 31-JAN-2023")

# --- Tests for fetch ---
@pytest.mark.asyncio
async def test_fetch_headers_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    uid = "123"
    header_data = b"From: test@example.com\r\nTo: user@example.com\r\nSubject: Test Email"
    # Corrected mock_response construction and escape sequence
    mock_response_payload = b'123 (BODY[HEADER.FIELDS (FROM TO CC SUBJECT DATE)]) {' + str(len(header_data)).encode() + b'}\r\n' + header_data + b'FLAGS (\\Seen)' # Escaped \Seen
    mock_response = ('OK', [(mock_response_payload, b')')])
    mock_imap_instance.uid.return_value = mock_response

    status, data = await client.fetch(uid, fetch_type='headers')

    assert status == 'OK'
    assert data is not None
    # The client.fetch method directly returns the server response. Parsing happens elsewhere.
    assert data == mock_response[1]
    mock_imap_instance.uid.assert_called_once_with('FETCH', uid, '(BODY.PEEK[HEADER.FIELDS (FROM TO CC SUBJECT DATE)])')

@pytest.mark.asyncio
async def test_fetch_full_email_successful(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    uid = "456"
    full_email_data = b"From: test@example.com\r\nTo: user@example.com\r\nSubject: Full Test\r\n\r\nThis is the body."
    # Corrected mock_response construction and escape sequence
    mock_response_payload_full = b'456 (BODY[]) {' + str(len(full_email_data)).encode() + b'}\r\n' + full_email_data + b'FLAGS (\\Seen)' # Escaped \Seen
    mock_response = ('OK', [(mock_response_payload_full, b')')])
    mock_imap_instance.uid.return_value = mock_response

    status, data = await client.fetch(uid, fetch_type='full')

    assert status == 'OK'
    assert data is not None
    assert data == mock_response[1]
    mock_imap_instance.uid.assert_called_once_with('FETCH', uid, '(BODY.PEEK[])')

@pytest.mark.asyncio
async def test_fetch_failure(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    uid = "789"
    mock_imap_instance.uid.return_value = ('NO', [b'Error fetching email'])

    status, data = await client.fetch(uid, fetch_type='full')

    assert status == 'NO'
    # Data might contain error message or be None, depending on imaplib behavior for your client
    assert data == [b'Error fetching email']

# --- Test for close ---
@pytest.mark.asyncio
async def test_close_connection(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.imap = mock_imap_instance # Ensure imap is set
    client.current_mailbox = "INBOX" # Simulate a mailbox was selected

    # Mock close and logout methods
    mock_imap_instance.close.return_value = ('OK', [b'Closed'])
    mock_imap_instance.logout.return_value = ('OK', [b'Logged out'])

    with patch('builtins.print') as mock_print:
        await client.close()

    mock_imap_instance.close.assert_called_once()
    mock_imap_instance.logout.assert_called_once()
    assert client.imap is None
    assert client.current_mailbox is None
    mock_print.assert_any_call("Closing IMAP connection...")
    mock_print.assert_any_call("IMAP connection closed.")

@pytest.mark.asyncio
async def test_close_connection_no_selected_mailbox(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.imap = mock_imap_instance
    client.current_mailbox = None # No mailbox was selected

    mock_imap_instance.logout.return_value = ('OK', [b'Logged out'])

    with patch('builtins.print') as mock_print:
        await client.close()

    mock_imap_instance.close.assert_not_called() # close() on imap connection should not be called if no mailbox open
    mock_imap_instance.logout.assert_called_once()
    assert client.imap is None

@pytest.mark.asyncio
async def test_close_connection_logout_error(mock_imap_client):
    client, mock_imap_instance = mock_imap_client
    client.imap = mock_imap_instance
    client.current_mailbox = "INBOX"

    mock_imap_instance.close.return_value = ('OK', [b'Closed'])
    mock_imap_instance.logout.side_effect = imaplib.IMAP4.error("Logout failed")

    with patch('builtins.print') as mock_print:
        await client.close() # Should still complete and set client.imap to None
    
    mock_imap_instance.close.assert_called_once()
    mock_imap_instance.logout.assert_called_once()
    assert client.imap is None # Should be reset even if logout fails
    mock_print.assert_any_call("Error closing IMAP connection: Logout failed")
