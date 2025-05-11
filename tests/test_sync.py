# tests/test_sync.py
import pytest
import pytest_asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import datetime

from sync import EmailSyncer, sync_email_headers, sync_full_emails, sync_attachments
from db import DatabaseManager 
from imap_client import ImapClient
from checkpoint import CheckpointManager as ActualCheckpointManager

@pytest.fixture
def mock_db_manager():
    """Provides a MagicMock for the DatabaseManager."""
    mock = MagicMock(spec=DatabaseManager)
    mock.log_sync_start = AsyncMock(return_value=1) 
    mock.log_sync_end = AsyncMock()
    mock.save_email_header = AsyncMock()
    mock.save_full_email = AsyncMock()
    mock.get_max_synced_header_uid = AsyncMock(return_value=0)
    mock.get_uids_for_full_sync = AsyncMock(return_value=[])
    mock.commit_with_retry = AsyncMock()
    return mock

@pytest.fixture
def mock_imap_client():
    """Provides a MagicMock for the ImapClient."""
    mock = MagicMock(spec=ImapClient)
    mock.select_mailbox = AsyncMock(return_value=True)
    mock.fetch = AsyncMock(return_value=('OK', [(b'HEADER', b'mock_header_data')])) 
    mock.search_all_uids_since = AsyncMock(return_value=[])
    mock.search_by_date_chunks = AsyncMock(return_value=[])
    return mock

@pytest_asyncio.fixture
async def mock_checkpoint_manager(): # For direct injection if needed, or spec for patching
    """Provides an AsyncMock for CheckpointManager (for patching)."""
    mock = AsyncMock(spec=ActualCheckpointManager)
    mock.last_uid = 0
    mock.failed_uids = {}
    mock.in_progress = False
    mock.timestamp = None
    mock.mark_start = AsyncMock()
    mock.mark_complete = AsyncMock()
    mock.update_progress = AsyncMock()
    mock.add_failed_uid = AsyncMock()
    mock.clear_failed_uid = AsyncMock()
    mock.save_state = AsyncMock()
    mock.get_last_uid = AsyncMock(return_value=0)
    mock.get_failed_uids_with_counts = AsyncMock(return_value={})
    mock.get_uids_to_retry = AsyncMock(return_value=[])
    mock.get_permanently_failed_uids = AsyncMock(return_value=[])
    return mock

@pytest.mark.asyncio
async def test_email_syncer_initialization(mock_db_manager, mock_imap_client):
    """Test basic initialization of EmailSyncer."""
    mailbox = "INBOX"
    mode = "headers"
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(
            db_manager=mock_db_manager, 
            imap_client=mock_imap_client, 
            mode=mode, 
            mailbox=mailbox
        )
        assert syncer.db_manager == mock_db_manager
        assert syncer.imap_client == mock_imap_client
        assert syncer.mode == mode
        assert syncer.checkpoint == mock_cm_instance
        PatchedCheckpointManager.assert_called_once_with(mock_db_manager, mode, mailbox)
        await syncer._initialize_failed_uids_cache()
        mock_cm_instance.get_failed_uids_with_counts.assert_called_once()
        assert syncer.failed_uids_counts == {}

@pytest.mark.asyncio
async def test_email_syncer_start_and_finish_sync(mock_db_manager, mock_imap_client):
    """Test the start_sync and finish_sync methods of EmailSyncer."""
    mailbox = "TEST_SYNC_BOX"
    mode = "headers"
    sync_start_message = f"Starting {mode} sync for {mailbox}"
    sync_finish_status = "COMPLETED"
    sync_finish_message = "Sync completed successfully"
    expected_status_id = 12345
    mock_db_manager.log_sync_start.return_value = expected_status_id
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={"uid1": 1})
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(
            db_manager=mock_db_manager, 
            imap_client=mock_imap_client, 
            mode=mode, 
            mailbox=mailbox
        )
        await syncer.start_sync(sync_start_message)
        mock_cm_instance.get_failed_uids_with_counts.assert_called_once()
        assert syncer.failed_uids_counts == {"uid1": 1}
        mock_cm_instance.mark_start.assert_called_once()
        mock_db_manager.log_sync_start.assert_called_once_with(sync_start_message)
        assert syncer.last_status_id == expected_status_id
        await syncer.finish_sync(sync_finish_status, sync_finish_message)
        mock_db_manager.log_sync_end.assert_called_once_with(
            expected_status_id, sync_finish_status, sync_finish_message
        )
        mock_cm_instance.mark_complete.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_success(mock_db_manager, mock_imap_client):
    """Test successful processing of email headers."""
    mailbox = "INBOX_HEADERS"
    mode = "headers"
    uid = "123"
    mock_header_bytes = (
        b"From: test@example.com\r\n"
        b"To: recv@example.com\r\n"
        b"Subject: Test Email\r\n"
        b"Date: Mon, 1 Jan 2023 12:00:00 +0000\r\n"
        b"\r\n"
        b"Body here"
    )
    mock_imap_client.fetch.return_value = ('OK', [(b'RFC822.HEADER', mock_header_bytes), b')'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field') as mock_decode_field, \
         patch('sync.parse_email_date') as mock_parse_email_date:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        mock_decode_field.side_effect = lambda x: x if x is not None else '' 
        mock_parse_email_date.return_value = "2023-01-01T12:00:00"
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_headers(uid, mailbox)
        assert result == 'saved'
        mock_imap_client.fetch.assert_called_once_with(uid, 'headers')
        mock_db_manager.save_email_header.assert_called_once()
        call_args = mock_db_manager.save_email_header.call_args
        assert call_args is not None
        args, kwargs = call_args
        assert kwargs['uid'] == uid
        assert kwargs['mailbox'] == mailbox
        assert 'subject' in kwargs 
        assert kwargs['msg_date'] == "2023-01-01T12:00:00"
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_fetch_failed(mock_db_manager, mock_imap_client):
    """Test processing headers when IMAP fetch fails."""
    mailbox = "INBOX_FAIL"
    mode = "headers"
    uid = "456"
    mock_imap_client.fetch.return_value = ('NO', [b'Error fetching'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_headers(uid, mailbox)
        assert result == 'fail'
        mock_imap_client.fetch.assert_called_once_with(uid, 'headers')
        mock_db_manager.save_email_header.assert_not_called()
        mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
        assert syncer.failed_uids_counts[uid] == 1
        mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_no_header_data(mock_db_manager, mock_imap_client):
    """Test processing headers when fetch is OK but no header data is found."""
    mailbox = "INBOX_NO_DATA"
    mode = "headers"
    uid = "789"
    mock_imap_client.fetch.return_value = ('OK', [(b'RFC822.HEADER', b''), b')'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_headers(uid, mailbox)
        assert result == 'fail'
        mock_imap_client.fetch.assert_called_once_with(uid, 'headers')
        mock_db_manager.save_email_header.assert_not_called()
        mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
        assert syncer.failed_uids_counts[uid] == 1
        mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_previously_failed_success(mock_db_manager, mock_imap_client):
    """Test successful processing of a header that was previously marked as failed."""
    mailbox = "INBOX_RETRY"
    mode = "headers"
    uid = "101"
    mock_header_bytes = (
        b"From: retry@example.com\r\n"
        b"Subject: Retry Success\r\n"
        b"Date: Tue, 2 Jan 2023 10:00:00 +0000\r\n"
        b"\r\nBody"
    )
    mock_imap_client.fetch.return_value = ('OK', [(b'RFC822.HEADER', mock_header_bytes), b')'])
    initial_failed_uids = {uid: 1}
    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field') as mock_decode_field, \
         patch('sync.parse_email_date') as mock_parse_email_date:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value=initial_failed_uids.copy())
        PatchedCheckpointManager.return_value = mock_cm_instance
        mock_decode_field.side_effect = lambda x: x if x is not None else ''
        mock_parse_email_date.return_value = "2023-01-02T10:00:00"
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        assert syncer.failed_uids_counts == initial_failed_uids
        result = await syncer.process_headers(uid, mailbox)
        assert result == 'saved'
        mock_db_manager.save_email_header.assert_called_once()
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_called_once_with(uid)
        assert uid not in syncer.failed_uids_counts
        mock_cm_instance.add_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_success(mock_db_manager, mock_imap_client):
    """Test successful processing of a full email."""
    mailbox = "INBOX_FULL"
    mode = "full"
    uid = "201"
    mock_raw_email_bytes = b"From: full@example.com\nSubject: Full Email Test\n\nThis is the full body."
    mock_imap_client.fetch.return_value = ('OK', [(b'BODY[]', mock_raw_email_bytes), b')'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.datetime') as mock_datetime:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        mock_now = datetime.datetime(2023, 1, 1, 12, 30, 0)
        mock_datetime.datetime.now.return_value = mock_now
        iso_timestamp = mock_now.isoformat()
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_full_email(uid, mailbox)
        assert result == 'saved'
        mock_imap_client.fetch.assert_called_once_with(uid, 'full')
        mock_db_manager.save_full_email.assert_called_once()
        args, kwargs = mock_db_manager.save_full_email.call_args
        assert kwargs['uid'] == uid
        assert kwargs['mailbox'] == mailbox
        assert kwargs['raw_email'] == mock_raw_email_bytes
        assert kwargs['fetched_at'] == iso_timestamp
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_fetch_failed(mock_db_manager, mock_imap_client):
    """Test processing full email when IMAP fetch fails."""
    mailbox = "INBOX_FULL_FAIL"
    mode = "full"
    uid = "202"
    mock_imap_client.fetch.return_value = ('NO', [b'Error fetching full email'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_full_email(uid, mailbox)
        assert result == 'fail'
        mock_imap_client.fetch.assert_called_once_with(uid, 'full')
        mock_db_manager.save_full_email.assert_not_called()
        mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
        assert syncer.failed_uids_counts[uid] == 1
        mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_no_raw_data(mock_db_manager, mock_imap_client):
    """Test processing full email when fetch is OK but no raw email data is found."""
    mailbox = "INBOX_FULL_NO_DATA"
    mode = "full"
    uid = "203"
    mock_imap_client.fetch.return_value = ('OK', [(b'BODY[]', b''), b')'])
    with patch('sync.CheckpointManager') as PatchedCheckpointManager:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        PatchedCheckpointManager.return_value = mock_cm_instance
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        result = await syncer.process_full_email(uid, mailbox)
        assert result == 'fail'
        mock_imap_client.fetch.assert_called_once_with(uid, 'full')
        mock_db_manager.save_full_email.assert_not_called()
        mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
        assert syncer.failed_uids_counts[uid] == 1
        mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_previously_failed_success(mock_db_manager, mock_imap_client):
    """Test successful processing of a full email that was previously marked as failed."""
    mailbox = "INBOX_FULL_RETRY"
    mode = "full"
    uid = "204"
    mock_raw_email_bytes = b"From: full_retry@example.com\nSubject: Full Retry Test\n\nRetry body."
    mock_imap_client.fetch.return_value = ('OK', [(b'BODY[]', mock_raw_email_bytes), b')'])
    initial_failed_uids = {uid: 1}
    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.datetime') as mock_datetime:
        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value=initial_failed_uids.copy())
        PatchedCheckpointManager.return_value = mock_cm_instance
        mock_now = datetime.datetime(2023, 1, 1, 13, 0, 0)
        mock_datetime.datetime.now.return_value = mock_now
        iso_timestamp = mock_now.isoformat()
        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        assert syncer.failed_uids_counts == initial_failed_uids
        result = await syncer.process_full_email(uid, mailbox)
        assert result == 'saved'
        mock_db_manager.save_full_email.assert_called_once()
        args, kwargs = mock_db_manager.save_full_email.call_args
        assert kwargs['uid'] == uid
        assert kwargs['raw_email'] == mock_raw_email_bytes
        assert kwargs['fetched_at'] == iso_timestamp
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_called_once_with(uid)
        assert uid not in syncer.failed_uids_counts
        mock_cm_instance.add_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_run_headers_success(mock_db_manager, mock_imap_client):
    """Test EmailSyncer.run() for successful header synchronization of a small batch."""
    mailbox1 = "INBOX"
    mailbox2 = "SENT"
    mode = "headers"
    fetch_mode_desc = "email headers"
    uids_to_fetch_tuples = [
        ("1", mailbox1),
        ("2", mailbox1),
        ("3", mailbox2)
    ]
    total_initial_count = len(uids_to_fetch_tuples)
    actual_processing_count = len(uids_to_fetch_tuples)

    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else '') as mock_decode, \
         patch('sync.parse_email_date', return_value="2023-01-01T00:00:00") as mock_parse_date, \
         patch('sync.tqdm', return_value=MagicMock()) as mock_tqdm:

        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        # Initialize attributes that CheckpointManager instance would have
        mock_cm_instance.last_uid = 0 
        mock_cm_instance.failed_uids = {}
        mock_cm_instance.in_progress = False
        mock_cm_instance.timestamp = None
        
        # Mock methods
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        mock_cm_instance.update_progress = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock()
        mock_cm_instance.clear_failed_uid = AsyncMock()
        
        # Crucially, ensure the save_state on the instance is a mock we can count and control for this test
        mock_cm_instance.save_state = AsyncMock() 
        
        PatchedCheckpointManager.return_value = mock_cm_instance

        # Configure side effects for mark_start and mark_complete to call the instance's save_state
        async def mock_mark_start_side_effect(*args, **kwargs):
            # Simulate internal behavior of CheckpointManager.mark_start
            mock_cm_instance.in_progress = True
            mock_cm_instance.timestamp = "dummy_start_timestamp" # or mock datetime.now().isoformat()
            await mock_cm_instance.save_state() 
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect

        async def mock_mark_complete_side_effect(*args, **kwargs):
            # Simulate internal behavior of CheckpointManager.mark_complete
            mock_cm_instance.in_progress = False
            mock_cm_instance.timestamp = "dummy_complete_timestamp"
            await mock_cm_instance.save_state()
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect
        
        syncer = EmailSyncer(
            db_manager=mock_db_manager, 
            imap_client=mock_imap_client, 
            mode=mode, 
            mailbox=mailbox1 
        )
        
        # Reset direct mocks on db_manager and imap_client before the test sequence
        # The mock_cm_instance is fresh from PatchedCheckpointManager for each test run of EmailSyncer
        mock_db_manager.reset_mock()
        mock_imap_client.reset_mock()

        # Re-establish necessary return values or side effects after reset for this specific test flow
        mock_db_manager.log_sync_start.return_value = 1 
        mock_imap_client.select_mailbox.return_value = True
        mock_imap_client.fetch.side_effect = [
            ('OK', [(b'RFC822.HEADER', b'Header data for 1')]),
            ('OK', [(b'RFC822.HEADER', b'Header data for 2')]),
            ('OK', [(b'RFC822.HEADER', b'Header data for 3')])
        ]
        # mock_cm_instance is already configured with its side effects for mark_start/complete

        # Start the sync process (calls mark_start -> save_state once)
        await syncer.start_sync(f"Starting {mode} sync for test run")
        # Assertions for start_sync effects
        mock_db_manager.log_sync_start.assert_called_once()
        mock_cm_instance.get_failed_uids_with_counts.assert_called_once() # From _initialize_failed_uids_cache
        mock_cm_instance.mark_start.assert_called_once() # This will have triggered one save_state call

        # Run the main processing loop
        processed_count, saved_count = await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

        assert processed_count == 3
        assert saved_count == 3

        assert mock_imap_client.select_mailbox.call_count == 2 # mailbox1, then mailbox2
        mock_imap_client.select_mailbox.assert_any_call(mailbox1)
        mock_imap_client.select_mailbox.assert_any_call(mailbox2)

        assert mock_imap_client.fetch.call_count == 3
        mock_db_manager.save_email_header.call_count == 3
        assert mock_cm_instance.update_progress.call_count == 3
        
        # Expected calls to checkpoint.save_state():
        # 1. From syncer.start_sync() -> (mocked)checkpoint.mark_start() -> (mocked)checkpoint.save_state()
        # 2. From end of the first (and only) chunk in syncer.run() -> self.checkpoint.save_state()
        # 3. From syncer.finish_sync() (called at end of run) -> (mocked)checkpoint.mark_complete() -> (mocked)checkpoint.save_state()
        assert mock_cm_instance.save_state.call_count == 3 
        
        # commit_with_retry calls: 1 from end-of-chunk, 1 from end-of-run
        assert mock_db_manager.commit_with_retry.call_count == 2
        
        # finish_sync effects
        mock_db_manager.log_sync_end.assert_called_once()
        mock_cm_instance.mark_complete.assert_called_once()

        # tqdm assertions
        mock_tqdm.assert_called_once_with(total=actual_processing_count, desc=f'Fetching {fetch_mode_desc}')
        tqdm_instance = mock_tqdm.return_value
        assert tqdm_instance.update.call_count == 3
        tqdm_instance.close.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_run_select_mailbox_fails(mock_db_manager, mock_imap_client):
    """Test EmailSyncer.run() when selecting a mailbox fails for a UID."""
    mailbox1 = "INBOX"
    mailbox_fails = "FAILBOX" # This mailbox selection will fail
    mailbox3 = "SENT"
    mode = "headers"
    fetch_mode_desc = "header sync with select fail"

    uids_to_fetch_tuples = [
        ("1", mailbox1),          # Success
        ("2", mailbox_fails),     # Select for this will fail
        ("3", mailbox_fails),     # Still in failed mailbox, should also skip fetch
        ("4", mailbox3)           # Success in a new mailbox
    ]
    total_initial_count = len(uids_to_fetch_tuples)
    actual_processing_count = len(uids_to_fetch_tuples)

    # Configure mock_imap_client.select_mailbox behavior
    # It should succeed for mailbox1 and mailbox3, fail for mailbox_fails
    async def select_mailbox_side_effect(mbox_name, read_only=False):
        if mbox_name == mailbox_fails:
            return False
        return True
    mock_imap_client.select_mailbox.side_effect = select_mailbox_side_effect

    # Configure imap_client.fetch for successful UIDs
    mock_imap_client.fetch.side_effect = [
        ('OK', [(b'RFC822.HEADER', b'Header data for 1')]), # For UID 1
        # No fetch for UID 2 or 3
        ('OK', [(b'RFC822.HEADER', b'Header data for 4')]), # For UID 4
    ]

    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else '') as mock_decode, \
         patch('sync.parse_email_date', return_value="2023-01-01T00:00:00") as mock_parse_date, \
         patch('sync.tqdm', return_value=MagicMock()) as mock_tqdm:

        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.last_uid = 0
        mock_cm_instance.failed_uids = {}
        mock_cm_instance.in_progress = False
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        mock_cm_instance.update_progress = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock() # Important for this test
        mock_cm_instance.clear_failed_uid = AsyncMock()
        mock_cm_instance.save_state = AsyncMock()
        PatchedCheckpointManager.return_value = mock_cm_instance

        async def mock_mark_start_side_effect(*args, **kwargs):
            await mock_cm_instance.save_state()
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        async def mock_mark_complete_side_effect(*args, **kwargs):
            await mock_cm_instance.save_state()
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect

        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox1)
        
        # Reset mocks before run
        mock_db_manager.reset_mock()
        mock_imap_client.reset_mock() # This clears side_effects too, so re-apply them
        mock_imap_client.select_mailbox.side_effect = select_mailbox_side_effect
        mock_imap_client.fetch.side_effect = [
            ('OK', [(b'RFC822.HEADER', b'Header data for 1')]),
            ('OK', [(b'RFC822.HEADER', b'Header data for 4')]),
        ]

        mock_cm_instance.reset_mock()
        mock_cm_instance.save_state = AsyncMock() # Re-attach mock for counting
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect
        mock_cm_instance.get_failed_uids_with_counts.return_value = {}

        await syncer.start_sync(f"Starting {mode} sync with select fail test")

        processed_count, saved_count = await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

        assert saved_count == 2 # Only UIDs 1 and 4 should be saved
        assert processed_count == 2 # run() currently counts successfully saved items as processed

        # Mailbox selection attempts: mailbox1 (initial), mailbox_fails, mailbox_fails again, mailbox3
        assert mock_imap_client.select_mailbox.call_count == 4 # Corrected expectation
        mock_imap_client.select_mailbox.assert_any_call(mailbox1)
        mock_imap_client.select_mailbox.assert_any_call(mailbox_fails)
        mock_imap_client.select_mailbox.assert_any_call(mailbox_fails)
        mock_imap_client.select_mailbox.assert_any_call(mailbox3)

        # Fetch calls only for UIDs 1 and 4
        assert mock_imap_client.fetch.call_count == 2
        mock_imap_client.fetch.assert_any_call("1", 'headers')
        mock_imap_client.fetch.assert_any_call("4", 'headers')

        # Save calls only for UIDs 1 and 4
        assert mock_db_manager.save_email_header.call_count == 2

        # add_failed_uid should be called for UIDs 2 and 3
        assert mock_cm_instance.add_failed_uid.call_count == 2
        mock_cm_instance.add_failed_uid.assert_any_call("2")
        mock_cm_instance.add_failed_uid.assert_any_call("3")
        assert syncer.failed_uids_counts["2"] >= 1
        assert syncer.failed_uids_counts["3"] >= 1

        # update_progress should only be called for successfully processed UIDs (1 and 4)
        assert mock_cm_instance.update_progress.call_count == 2 

        # tqdm update should be called for all UIDs attempted
        tqdm_instance = mock_tqdm.return_value
        assert tqdm_instance.update.call_count == len(uids_to_fetch_tuples)

        # Check completion status (should still complete the run)
        mock_db_manager.log_sync_end.assert_called_once()
        mock_cm_instance.mark_complete.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_run_process_headers_fails_for_uid(mock_db_manager, mock_imap_client):
    """Test EmailSyncer.run() when process_headers fails for a specific UID."""
    mailbox = "INBOX"
    mode = "headers"
    fetch_mode_desc = "header sync with item fail"

    uids_to_fetch_tuples = [
        ("10", mailbox), # Success
        ("20", mailbox), # This one will fail during fetch for process_headers
        ("30", mailbox)  # Success
    ]
    total_initial_count = len(uids_to_fetch_tuples)
    actual_processing_count = len(uids_to_fetch_tuples)

    # Configure imap_client.fetch to fail for UID "20"
    async def fetch_side_effect(uid, type):
        if uid == "20" and type == "headers":
            return ('NO', [b'Fetch error for UID 20']) # Causes process_headers to return 'fail'
        elif type == "headers":
            return ('OK', [(b'RFC822.HEADER', f'Header data for {uid}'.encode())])
        return ('UNKNOWN', []) # Should not be called for other types
    mock_imap_client.fetch.side_effect = fetch_side_effect

    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else '') as mock_decode, \
         patch('sync.parse_email_date', return_value="2023-01-01T00:00:00") as mock_parse_date, \
         patch('sync.tqdm', return_value=MagicMock()) as mock_tqdm:

        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        # Standard setup for mock_cm_instance for a run test
        mock_cm_instance.last_uid = 0; mock_cm_instance.failed_uids = {}; mock_cm_instance.in_progress = False
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        mock_cm_instance.update_progress = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock()
        mock_cm_instance.clear_failed_uid = AsyncMock()
        mock_cm_instance.save_state = AsyncMock()
        PatchedCheckpointManager.return_value = mock_cm_instance

        async def mock_mark_start_side_effect(*args, **kwargs): await mock_cm_instance.save_state()
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        async def mock_mark_complete_side_effect(*args, **kwargs): await mock_cm_instance.save_state()
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect

        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        
        # Reset and re-configure mocks specific to this test run
        mock_db_manager.reset_mock()
        mock_imap_client.reset_mock() # Clears select_mailbox and fetch side_effects
        mock_imap_client.select_mailbox.return_value = True # All selects succeed
        mock_imap_client.fetch.side_effect = fetch_side_effect # Re-apply specific fetch logic
        
        mock_cm_instance.reset_mock()
        mock_cm_instance.save_state = AsyncMock()
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect
        mock_cm_instance.get_failed_uids_with_counts.return_value = {}
        # add_failed_uid will be called by process_headers, so we need to ensure it is a mock
        mock_cm_instance.add_failed_uid = AsyncMock() 

        await syncer.start_sync(f"Starting {mode} sync with item fail test")

        processed_count, saved_count = await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

        assert saved_count == 2 # UIDs 10 and 30 saved
        assert processed_count == 2 # Only successful saves are counted in processed_count by run()

        # Mailbox selection: only once for "INBOX"
        assert mock_imap_client.select_mailbox.call_count == 1
        mock_imap_client.select_mailbox.assert_called_once_with(mailbox)

        # Fetch calls for all UIDs (10, 20, 30)
        assert mock_imap_client.fetch.call_count == 3
        mock_imap_client.fetch.assert_any_call("10", 'headers')
        mock_imap_client.fetch.assert_any_call("20", 'headers') 
        mock_imap_client.fetch.assert_any_call("30", 'headers')

        # Save email header calls only for UIDs 10 and 30
        assert mock_db_manager.save_email_header.call_count == 2

        # checkpoint.add_failed_uid called by process_headers for UID "20"
        mock_cm_instance.add_failed_uid.assert_called_once_with("20")
        assert syncer.failed_uids_counts["20"] >= 1 

        # checkpoint.update_progress called only for UIDs 10 and 30
        assert mock_cm_instance.update_progress.call_count == 2
        mock_cm_instance.update_progress.assert_any_call("10")
        mock_cm_instance.update_progress.assert_any_call("30")

        tqdm_instance = mock_tqdm.return_value
        assert tqdm_instance.update.call_count == len(uids_to_fetch_tuples) # pbar.update is called for each item

        mock_db_manager.log_sync_end.assert_called_once()
        mock_cm_instance.mark_complete.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_run_emails_per_commit_triggers_save(mock_db_manager, mock_imap_client):
    """Test that EMAILS_PER_COMMIT triggers checkpoint save and DB commit during run()."""
    mailbox = "INBOX_COMMIT_TEST"
    mode = "headers"
    fetch_mode_desc = "header sync with commit trigger"

    # Patch EMAILS_PER_COMMIT to a small number for this test
    with patch('sync.EMAILS_PER_COMMIT', 2) as mock_emails_per_commit, \
         patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else '') as mock_decode, \
         patch('sync.parse_email_date', return_value="2023-01-01T00:00:00") as mock_parse_date, \
         patch('sync.tqdm', return_value=MagicMock()) as mock_tqdm:

        uids_to_fetch_tuples = [
            ("100", mailbox), # Processed, emails_since_commit = 1
            ("200", mailbox), # Processed, emails_since_commit = 2 -> triggers save/commit, reset to 0
            ("300", mailbox), # Processed, emails_since_commit = 1
            ("400", mailbox)  # Processed, emails_since_commit = 2 -> triggers save/commit, reset to 0
        ]
        total_initial_count = len(uids_to_fetch_tuples)
        actual_processing_count = len(uids_to_fetch_tuples)

        # All fetches succeed
        mock_imap_client.fetch.side_effect = [
            ('OK', [(b'RFC822.HEADER', b'H1')]),
            ('OK', [(b'RFC822.HEADER', b'H2')]),
            ('OK', [(b'RFC822.HEADER', b'H3')]),
            ('OK', [(b'RFC822.HEADER', b'H4')]),
        ]

        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.last_uid = 0; mock_cm_instance.failed_uids = {}; mock_cm_instance.in_progress = False
        mock_cm_instance.get_failed_uids_with_counts = AsyncMock(return_value={})
        mock_cm_instance.get_last_uid = AsyncMock(return_value=0)
        mock_cm_instance.update_progress = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock()
        mock_cm_instance.clear_failed_uid = AsyncMock()
        mock_cm_instance.save_state = AsyncMock()
        PatchedCheckpointManager.return_value = mock_cm_instance

        async def mock_mark_start_side_effect(*args, **kwargs): await mock_cm_instance.save_state()
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        async def mock_mark_complete_side_effect(*args, **kwargs): await mock_cm_instance.save_state()
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect

        syncer = EmailSyncer(db_manager=mock_db_manager, imap_client=mock_imap_client, mode=mode, mailbox=mailbox)
        
        # Reset mocks
        mock_db_manager.reset_mock(); mock_db_manager.log_sync_start.return_value = 1
        mock_imap_client.reset_mock(); mock_imap_client.select_mailbox.return_value = True
        mock_imap_client.fetch.side_effect = [ # Re-apply after reset
            ('OK', [(b'RFC822.HEADER', b'H1')]), ('OK', [(b'RFC822.HEADER', b'H2')]),
            ('OK', [(b'RFC822.HEADER', b'H3')]), ('OK', [(b'RFC822.HEADER', b'H4')]),
        ]
        mock_cm_instance.reset_mock(); mock_cm_instance.save_state = AsyncMock()
        mock_cm_instance.mark_start.side_effect = mock_mark_start_side_effect
        mock_cm_instance.mark_complete.side_effect = mock_mark_complete_side_effect
        mock_cm_instance.get_failed_uids_with_counts.return_value = {}

        await syncer.start_sync("Commit trigger test")
        
        # save_state calls before run: 1 (from mark_start)
        # db_commit calls before run: 0
        pre_run_save_state_calls = mock_cm_instance.save_state.call_count
        pre_run_db_commit_calls = mock_db_manager.commit_with_retry.call_count

        processed_count, saved_count = await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

        assert saved_count == 4
        assert processed_count == 4

        # EMAILS_PER_COMMIT saves/commits: 2 times (after UID 200, after UID 400)
        # End of chunk save/commit (CHUNK_SIZE=100, 4 items, 1 chunk): 1 time
        # Total save_state related to EMAILS_PER_COMMIT and end-of-chunk inside run: 2 + 1 = 3
        # Total db_commits related to EMAILS_PER_COMMIT and end-of-chunk inside run: 2 + 1 = 3
        
        # Calls to checkpoint.save_state():
        # 1 from start_sync (via mark_start)
        # 2 from EMAILS_PER_COMMIT rule during the loop
        # 1 from end-of-chunk rule during the loop (as chunk_idx % 1 == 0)
        # 1 from finish_sync (via mark_complete)
        # Total expected = 1 (pre_run) + 2 (EMAILS_PER_COMMIT) + 1 (end_chunk) + 1 (mark_complete) = 5
        assert mock_cm_instance.save_state.call_count == 5

        # Calls to db_manager.commit_with_retry():
        # 0 from start_sync
        # 2 from EMAILS_PER_COMMIT rule during the loop
        # 1 from end-of-chunk rule during the loop
        # 1 from end of run (final commit)
        # Total expected = 0 (pre_run) + 2 (EMAILS_PER_COMMIT) + 1 (end_chunk) + 1 (final_commit_in_run) = 4
        assert mock_db_manager.commit_with_retry.call_count == 4

        assert mock_db_manager.save_email_header.call_count == 4
        assert mock_cm_instance.update_progress.call_count == 4
        mock_db_manager.log_sync_end.assert_called_once()
        mock_cm_instance.mark_complete.assert_called_once()

@pytest.mark.asyncio
async def test_sync_email_headers_orchestrator(mock_db_manager, mock_imap_client):
    """Test the sync_email_headers orchestrator function."""
    mailbox = "INBOX_HEADERS_ORCH"
    # Ensure imap_client returns some UIDs to trigger syncer.run()
    mock_imap_client.search_all_uids_since = AsyncMock(return_value=['100', '101']) # Simulate UIDs found
    mock_imap_client.search_by_date_chunks = AsyncMock(return_value=['200', '201']) # For All Mail case

    with patch('sync.EmailSyncer') as MockEmailSyncer:
        mock_syncer_instance = AsyncMock(spec=EmailSyncer)

        mock_checkpoint_attr = AsyncMock(spec=ActualCheckpointManager)
        mock_checkpoint_attr.get_last_uid = AsyncMock(return_value=0)
        mock_checkpoint_attr.get_uids_to_retry = AsyncMock(return_value=[])
        mock_checkpoint_attr.get_permanently_failed_uids = AsyncMock(return_value=[])
        
        mock_syncer_instance.checkpoint = mock_checkpoint_attr

        mock_syncer_instance.start_sync = AsyncMock()
        mock_syncer_instance.finish_sync = AsyncMock()
        mock_syncer_instance.run = AsyncMock(return_value=(0,0))
        
        MockEmailSyncer.return_value = mock_syncer_instance

        await sync_email_headers(mock_db_manager, mock_imap_client, mailbox)

        MockEmailSyncer.assert_called_once_with(
            mock_db_manager,
            mock_imap_client,
            'headers',
            mailbox
        )
        mock_syncer_instance.start_sync.assert_called_once()
        mock_syncer_instance.run.assert_called_once()

        mock_syncer_instance.finish_sync.assert_called_once() # Should be called if run completes or no UIDs

        mock_checkpoint_attr.get_last_uid.assert_called_once()
        mock_checkpoint_attr.get_uids_to_retry.assert_called_once()
        mock_checkpoint_attr.get_permanently_failed_uids.assert_called_once()

@pytest.mark.asyncio
async def test_sync_full_emails_orchestrator(mock_db_manager, mock_imap_client):
    """Test the sync_full_emails orchestrator function."""
    mailbox = "INBOX_FULL_ORCH"
    # Ensure db_manager returns some UIDs to trigger syncer.run()
    mock_db_manager.get_uids_for_full_sync = AsyncMock(return_value=['300', '301'])

    with patch('sync.EmailSyncer') as MockEmailSyncer:
        mock_syncer_instance = AsyncMock(spec=EmailSyncer)

        mock_checkpoint_attr = AsyncMock(spec=ActualCheckpointManager)
        mock_checkpoint_attr.get_uids_to_retry = AsyncMock(return_value=[])
        mock_checkpoint_attr.get_permanently_failed_uids = AsyncMock(return_value=[])

        mock_syncer_instance.checkpoint = mock_checkpoint_attr
        mock_syncer_instance.start_sync = AsyncMock()
        mock_syncer_instance.finish_sync = AsyncMock()
        mock_syncer_instance.run = AsyncMock(return_value=(0,0))
        
        MockEmailSyncer.return_value = mock_syncer_instance

        mock_db_manager.get_uids_with_full_content = AsyncMock(return_value=[])

        await sync_full_emails(mock_db_manager, mock_imap_client, mailbox)

        MockEmailSyncer.assert_called_once_with(
            mock_db_manager,
            mock_imap_client,
            'full',
            mailbox
        )
        mock_syncer_instance.start_sync.assert_called_once()
        mock_syncer_instance.run.assert_called_once()
        mock_syncer_instance.finish_sync.assert_called_once()
        
        mock_db_manager.get_uids_for_full_sync.assert_called_once_with(mailbox)
        mock_db_manager.get_uids_with_full_content.assert_called_once_with(mailbox)
        mock_checkpoint_attr.get_uids_to_retry.assert_called_once()
        mock_checkpoint_attr.get_permanently_failed_uids.assert_called_once()

@pytest.mark.asyncio
async def test_sync_attachments_orchestrator(mock_db_manager):
    """Test the sync_attachments orchestrator function."""
    mailbox = "INBOX_ATTACH_ORCH"
    
    # Mock the data returned by get_full_emails_for_attachment_processing
    # Each row: uid_str, mbox, raw_content
    # Let's simulate one email with some dummy raw content that might have an attachment.
    mock_email_content = b'''From: test@example.com
Subject: Test with attachment
Content-Type: multipart/mixed; boundary=boundary

--boundary
Content-Type: text/plain

This is the body.
--boundary
Content-Type: application/octet-stream
Content-Disposition: attachment; filename="test.dat"

TestData
--boundary--
'''
    mock_db_manager.get_full_emails_for_attachment_processing = AsyncMock(return_value=[
        ('attach_uid1', mailbox, mock_email_content)
    ])
    # Other DB calls made by sync_attachments or its helpers:
    mock_db_manager.save_attachment_blob = AsyncMock()
    mock_db_manager.map_email_to_attachment = AsyncMock()
    mock_db_manager.log_sync_start = AsyncMock(return_value=99) # For the one in sync_attachments
    mock_db_manager.log_sync_end = AsyncMock()
    mock_db_manager.commit_with_retry = AsyncMock()
    mock_db_manager.get_unique_attachment_blob_count = AsyncMock(return_value=1)
    mock_db_manager.get_attachment_mappings_count_for_mailbox = AsyncMock(return_value=1)

    # sync_attachments instantiates its own CheckpointManager, so we patch the class.
    with patch('sync.CheckpointManager') as PatchedCheckpointManager, \
         patch('sync.tqdm', return_value=MagicMock()) as mock_tqdm: # tqdm is used in sync_attachments

        mock_cm_instance = AsyncMock(spec=ActualCheckpointManager)
        mock_cm_instance.mark_start = AsyncMock()
        mock_cm_instance.mark_complete = AsyncMock()
        mock_cm_instance.save_state = AsyncMock()
        mock_cm_instance.add_failed_uid = AsyncMock()
        mock_cm_instance.clear_failed_uid = AsyncMock()
        PatchedCheckpointManager.return_value = mock_cm_instance

        await sync_attachments(mock_db_manager, mailbox)

        # Assert that CheckpointManager was instantiated correctly and methods called
        PatchedCheckpointManager.assert_called_once_with(mock_db_manager, 'attachments', mailbox)
        mock_cm_instance.mark_start.assert_called_once()
        mock_cm_instance.save_state.assert_called() # Called at least once
        mock_cm_instance.mark_complete.assert_called_once()
        mock_cm_instance.clear_failed_uid.assert_called_once_with('attach_uid1') # Assuming successful processing of attach_uid1

        # Assert DB calls
        mock_db_manager.get_full_emails_for_attachment_processing.assert_called_once_with(mailbox)
        mock_db_manager.save_attachment_blob.assert_called() # Should be called for test.dat
        mock_db_manager.map_email_to_attachment.assert_called() # Should be called for test.dat
        mock_db_manager.log_sync_start.assert_called_once()
        mock_db_manager.log_sync_end.assert_called_once()
        mock_db_manager.commit_with_retry.assert_called() # Called at least once
        mock_tqdm.assert_called()        

# End of tests for sync_email_headers, sync_full_emails, sync_attachments