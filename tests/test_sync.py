# tests/test_sync.py
import pytest
import pytest_asyncio
from unittest.mock import MagicMock, AsyncMock, patch
import datetime

from sync import EmailSyncer, sync_email_headers, sync_full_emails, sync_attachments
from utils import CHUNK_SIZE, EMAILS_PER_COMMIT, debug_print, parse_email_date, decode_field

@pytest.mark.asyncio
async def test_email_syncer_initialization(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test basic initialization of EmailSyncer."""
    mailbox = "INBOX"
    mode = "headers"
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}
    mock_cm_instance.get_last_uid.return_value = 0

    syncer = EmailSyncer(
        db_manager=mocked_db_interface, 
        imap_client=mocked_imap_client_interface, 
        mode=mode, 
        mailbox=mailbox
    )
    assert syncer.db_manager == mocked_db_interface
    assert syncer.imap_client == mocked_imap_client_interface
    assert syncer.mode == mode
    assert syncer.checkpoint == mock_cm_instance
    
    await syncer._initialize_failed_uids_cache()
    mock_cm_instance.get_failed_uids_with_counts.assert_called_once()
    assert syncer.failed_uids_counts == {}

@pytest.mark.asyncio
async def test_email_syncer_start_and_finish_sync(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test the start_sync and finish_sync methods of EmailSyncer."""
    mailbox = "TEST_SYNC_BOX"
    mode = "headers"
    sync_start_message = f"Starting {mode} sync for {mailbox}"
    sync_finish_status = "COMPLETED"
    sync_finish_message = "Sync completed successfully"
    expected_status_id = 12345
    mocked_db_interface.log_sync_start.return_value = expected_status_id
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {"uid1": 1}

    syncer = EmailSyncer(
        db_manager=mocked_db_interface, 
        imap_client=mocked_imap_client_interface, 
        mode=mode, 
        mailbox=mailbox
    )
    await syncer.start_sync(sync_start_message)
    mock_cm_instance.get_failed_uids_with_counts.assert_called_once()
    assert syncer.failed_uids_counts == {"uid1": 1}
    mock_cm_instance.mark_start.assert_called_once()
    mocked_db_interface.log_sync_start.assert_called_once_with(sync_start_message)
    assert syncer.last_status_id == expected_status_id
    
    await syncer.finish_sync(sync_finish_status, sync_finish_message)
    mocked_db_interface.log_sync_end.assert_called_once_with(
        expected_status_id, sync_finish_status, sync_finish_message
    )
    mock_cm_instance.mark_complete.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_success(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
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
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'RFC822.HEADER', mock_header_bytes), b')'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    with patch('sync.decode_field') as mock_decode_field, \
         patch('sync.parse_email_date') as mock_parse_email_date:
        mock_decode_field.side_effect = lambda x: x if x is not None else '' 
        mock_parse_email_date.return_value = "2023-01-01T12:00:00"
        
        syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache()
        
        result = await syncer.process_headers(uid, mailbox)
        
        assert result == 'saved'
        mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'headers')
        mocked_db_interface.save_email_header.assert_called_once()
        call_args = mocked_db_interface.save_email_header.call_args
        assert call_args is not None
        args, kwargs = call_args
        assert kwargs['uid'] == uid
        assert kwargs['mailbox'] == mailbox
        assert 'subject' in kwargs 
        assert kwargs['msg_date'] == "2023-01-01T12:00:00"
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_fetch_failed(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test processing headers when IMAP fetch fails."""
    mailbox = "INBOX_FAIL"
    mode = "headers"
    uid = "456"
    mocked_imap_client_interface.fetch.return_value = ('NO', [b'Error fetching'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    
    result = await syncer.process_headers(uid, mailbox)
    
    assert result == 'fail'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'headers')
    mocked_db_interface.save_email_header.assert_not_called()
    mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
    assert syncer.failed_uids_counts[uid] == 1 
    mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_no_header_data(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test processing headers when fetch is OK but no header data is found."""
    mailbox = "INBOX_NO_DATA"
    mode = "headers"
    uid = "789"
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'RFC822.HEADER', b''), b')'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    
    result = await syncer.process_headers(uid, mailbox)
    
    assert result == 'fail'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'headers')
    mocked_db_interface.save_email_header.assert_not_called()
    mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
    assert syncer.failed_uids_counts[uid] == 1
    mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_headers_previously_failed_success(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
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
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'RFC822.HEADER', mock_header_bytes), b')'])
    initial_failed_uids_counts = {uid: 1}

    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = initial_failed_uids_counts.copy()

    with patch('sync.decode_field') as mock_decode_field, \
         patch('sync.parse_email_date') as mock_parse_email_date:
        mock_decode_field.side_effect = lambda x: x if x is not None else ''
        mock_parse_email_date.return_value = "2023-01-02T10:00:00"
        
        syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
        await syncer._initialize_failed_uids_cache() 
        assert syncer.failed_uids_counts == initial_failed_uids_counts
        
        result = await syncer.process_headers(uid, mailbox)
        
        assert result == 'saved'
        mocked_db_interface.save_email_header.assert_called_once()
        mock_cm_instance.update_progress.assert_called_once_with(uid)
        mock_cm_instance.clear_failed_uid.assert_called_once_with(uid)
        assert uid not in syncer.failed_uids_counts 
        mock_cm_instance.add_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_success(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager, mock_sync_datetime):
    """Test successful processing of a full email."""
    mailbox = "INBOX_FULL"
    mode = "full"
    uid = "201"
    mock_raw_email_bytes = b"From: full@example.com\nSubject: Full Email Test\n\nThis is the full body."
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'BODY[]', mock_raw_email_bytes), b')'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    iso_timestamp = mock_sync_datetime.datetime.now.return_value.isoformat()

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    
    result = await syncer.process_full_email(uid, mailbox)
    
    assert result == 'saved'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'full')
    mocked_db_interface.save_full_email.assert_called_once()
    args, kwargs = mocked_db_interface.save_full_email.call_args
    assert kwargs['uid'] == uid
    assert kwargs['mailbox'] == mailbox
    assert kwargs['raw_email'] == mock_raw_email_bytes
    assert kwargs['fetched_at'] == iso_timestamp
    mock_cm_instance.update_progress.assert_called_once_with(uid)
    mock_cm_instance.clear_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_fetch_failed(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test processing full email when IMAP fetch fails."""
    mailbox = "INBOX_FULL_FAIL"
    mode = "full"
    uid = "202"
    mocked_imap_client_interface.fetch.return_value = ('NO', [b'Error fetching full email'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    
    result = await syncer.process_full_email(uid, mailbox)
    
    assert result == 'fail'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'full')
    mocked_db_interface.save_full_email.assert_not_called()
    mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
    assert syncer.failed_uids_counts[uid] == 1
    mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_no_raw_data(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test processing full email when fetch is OK but no raw data is found."""
    mailbox = "INBOX_FULL_NO_DATA"
    mode = "full"
    uid = "203"
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'BODY[]', b''), b')'])
    
    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = {}

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    
    result = await syncer.process_full_email(uid, mailbox)
    
    assert result == 'fail'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'full')
    mocked_db_interface.save_full_email.assert_not_called()
    mock_cm_instance.add_failed_uid.assert_called_once_with(uid)
    assert syncer.failed_uids_counts[uid] == 1
    mock_cm_instance.update_progress.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_process_full_email_previously_failed_success(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager, mock_sync_datetime):
    """Test successful processing of a full email that was previously failed."""
    mailbox = "INBOX_FULL_RETRY"
    mode = "full"
    uid = "204"
    mock_raw_email_bytes = b"Retry body"
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'BODY[]', mock_raw_email_bytes), b')'])
    initial_failed_uids_counts = {uid: 2}

    mock_cm_instance = patched_checkpoint_manager
    mock_cm_instance.get_failed_uids_with_counts.return_value = initial_failed_uids_counts.copy()
    
    iso_timestamp = mock_sync_datetime.datetime.now.return_value.isoformat()

    syncer = EmailSyncer(db_manager=mocked_db_interface, imap_client=mocked_imap_client_interface, mode=mode, mailbox=mailbox)
    await syncer._initialize_failed_uids_cache()
    assert syncer.failed_uids_counts == initial_failed_uids_counts
    
    result = await syncer.process_full_email(uid, mailbox)
    
    assert result == 'saved'
    mocked_imap_client_interface.fetch.assert_called_once_with(uid, 'full')
    mocked_db_interface.save_full_email.assert_called_once_with(
        uid=uid, 
        mailbox=mailbox, 
        raw_email=mock_raw_email_bytes,
        fetched_at=iso_timestamp
    )
    mock_cm_instance.update_progress.assert_called_once_with(uid)
    mock_cm_instance.clear_failed_uid.assert_called_once_with(uid)
    assert uid not in syncer.failed_uids_counts
    mock_cm_instance.add_failed_uid.assert_not_called()

@pytest.mark.asyncio
async def test_email_syncer_run_headers_success(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test EmailSyncer.run for header sync, successful processing of some UIDs."""
    mailbox = "RUN_HEADERS_SUCCESS"
    mode = "headers"
    start_uid_from_checkpoint = 100
    uids_from_imap = [str(i) for i in range(101, 106)] # 101, 102, 103, 104, 105
    uids_to_retry = ["90", "95"] # Will be processed first

    mock_cm = patched_checkpoint_manager
    mock_cm.get_last_uid.return_value = start_uid_from_checkpoint
    mock_cm.get_uids_to_retry.return_value = uids_to_retry
    mock_cm.get_failed_uids_with_counts.return_value = {uid:1 for uid in uids_to_retry} # Syncer init

    mocked_imap_client_interface.search_all_uids_since.return_value = uids_from_imap
    
    mock_header_bytes = b"Subject: Test Header\r\n"
    async def mock_fetch_side_effect(uid, type):
        if type == 'headers':
            if uid == "90": 
                return ('NO', [b'Simulated fetch error for 90'])
            return ('OK', [(b'RFC822.HEADER', mock_header_bytes), b')'])
        return ('NO', [b'Unexpected fetch type'])
    mocked_imap_client_interface.fetch.side_effect = mock_fetch_side_effect

    mocked_db_interface.log_sync_start.return_value = 999

    syncer = EmailSyncer(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mode=mode,
        mailbox=mailbox
    )

    # Prepare arguments for syncer.run()
    all_uids_to_process = uids_to_retry + uids_from_imap
    uids_to_fetch_tuples = [(uid, mailbox) for uid in all_uids_to_process]
    total_initial_count = len(all_uids_to_process)
    fetch_mode_desc = f"{mode} for {mailbox}"

    with patch('sync.parse_email_date', return_value="parsed-date"), \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else ''):
        
        await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True)
    
    assert mocked_imap_client_interface.fetch.call_count >= len(uids_to_retry) + len(uids_from_imap) -1 # -1 for failed "90"

    mocked_imap_client_interface.fetch.assert_any_call("90", 'headers')
    mocked_imap_client_interface.fetch.assert_any_call("95", 'headers')

    mock_cm.add_failed_uid.assert_any_call("90") 
    mock_cm.clear_failed_uid.assert_any_call("95")
    
    for uid in uids_from_imap:
        mocked_imap_client_interface.fetch.assert_any_call(uid, 'headers')
        mock_cm.update_progress.assert_any_call(uid)

    assert mocked_db_interface.save_email_header.call_count == 1 + len(uids_from_imap)

    if mocked_db_interface.save_email_header.call_count > 0:
        expected_commits = (mocked_db_interface.save_email_header.call_count + EMAILS_PER_COMMIT - 1) // EMAILS_PER_COMMIT
        assert mocked_db_interface.commit_with_retry.call_count >= expected_commits

    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()

@pytest.mark.asyncio
async def test_email_syncer_run_select_mailbox_fails(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test EmailSyncer.run when select_mailbox fails."""
    mailbox = "UNREACHABLE_MAILBOX"
    mode = "headers"

    mock_cm = patched_checkpoint_manager
    mocked_imap_client_interface.select_mailbox.return_value = False
    mocked_db_interface.log_sync_start.return_value = 1000

    syncer = EmailSyncer(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mode=mode,
        mailbox=mailbox
    )

    # Prepare arguments for syncer.run()
    # In this case, select_mailbox fails, so run might not process many UIDs.
    # The UIDs to fetch would typically come from checkpoint or IMAP search.
    # Provide one UID to ensure the loop is entered and select_mailbox is attempted.
    uids_to_fetch_tuples = [("1", mailbox)]
    total_initial_count = 1
    fetch_mode_desc = f"{mode} for {mailbox}"

    await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True)
    # mocked_db_interface.log_sync_start.assert_called_once() # log_sync_start is called by orchestrator's start_sync
    
    mocked_imap_client_interface.search_all_uids_since.assert_not_called()
    mocked_imap_client_interface.fetch.assert_not_called()
    
    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    pos_args, kw_args = call_args_list[0]
    assert pos_args[1] == "COMPLETED_NO_CHANGES" # status
    assert pos_args[2] == f"No new {mode} for {mailbox} saved despite processing 0 UIDs"

@pytest.mark.asyncio
async def test_email_syncer_run_process_headers_fails_for_uid(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test EmailSyncer.run when process_headers consistently fails for a UID."""
    mailbox = "PROCESS_FAIL_MAILBOX"
    mode = "headers"
    start_uid = 0
    uids_from_imap = ["10", "11", "12"] # "11" will fail processing

    mock_cm = patched_checkpoint_manager
    mock_cm.get_last_uid.return_value = start_uid
    mock_cm.get_uids_to_retry.return_value = []
    mock_cm.get_failed_uids_with_counts.return_value = {} # For syncer init

    mocked_imap_client_interface.search_all_uids_since.return_value = uids_from_imap

    async def fetch_side_effect(uid, type):
        if uid == "11":
            return ('NO', [b'Fetch error for UID 11']) # Consistently fail for UID 11
        return ('OK', [(b'RFC822.HEADER', b"Subject: Test\r\n"), b')'])
    mocked_imap_client_interface.fetch.side_effect = fetch_side_effect
    
    mocked_db_interface.log_sync_start.return_value = 1001

    syncer = EmailSyncer(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mode=mode,
        mailbox=mailbox
    )
    with patch('sync.parse_email_date', return_value="parsed-date"), \
         patch('sync.decode_field', side_effect=lambda x: x if x is not None else ''):
        
        # Prepare arguments for syncer.run()
        uids_to_fetch_tuples = [(uid, mailbox) for uid in uids_from_imap]
        total_initial_count = len(uids_from_imap)
        fetch_mode_desc = f"{mode} for {mailbox}"
        
        await syncer.run(uids_to_fetch_tuples, total_initial_count, fetch_mode_desc)

    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True)
    # mocked_imap_client_interface.search_all_uids_since.assert_called_once_with(start_uid, CHUNK_SIZE, mailbox) # This is called by orchestrator, not by EmailSyncer.run()

    for uid in uids_from_imap:
        mocked_imap_client_interface.fetch.assert_any_call(uid, 'headers')

    mocked_db_interface.save_email_header.assert_any_call(uid="10", mailbox=mailbox, subject="Test", msg_date="parsed-date", msg_from="", msg_to="", msg_cc="")
    mocked_db_interface.save_email_header.assert_any_call(uid="12", mailbox=mailbox, subject="Test", msg_date="parsed-date", msg_from="", msg_to="", msg_cc="")
    assert mocked_db_interface.save_email_header.call_count == 2 

    mock_cm.update_progress.assert_any_call("10")
    mock_cm.update_progress.assert_any_call("12")
    # mock_cm.update_progress.assert_never_called_with("11") # Incorrect assertion method
    # Check that "11" was not passed to update_progress
    progress_calls = [call_args[0][0] for call_args in mock_cm.update_progress.call_args_list]
    assert "11" not in progress_calls

    mock_cm.add_failed_uid.assert_called_once_with("11")
    
    if mocked_db_interface.save_email_header.call_count > 0:
        expected_commits = (mocked_db_interface.save_email_header.call_count + EMAILS_PER_COMMIT - 1) // EMAILS_PER_COMMIT
        assert mocked_db_interface.commit_with_retry.call_count >= expected_commits

    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    pos_args, kw_args = call_args_list[0]
    assert pos_args[1] == "COMPLETED" # status, syncer.run doesn't check internal failed_uids for this
    assert f"Successfully processed 2 {mode} for {mailbox}" in pos_args[2] # message

@pytest.mark.asyncio
async def test_email_syncer_run_emails_per_commit_triggers_save(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test that checkpoint save is triggered based on EMAILS_PER_COMMIT."""
    mailbox = "COMMIT_TEST_MAILBOX"
    mode = "headers"
    uids_from_imap = [str(i) for i in range(1, EMAILS_PER_COMMIT * 2 + 1)] # e.g., if 50, then 100 UIDs

    mock_cm = patched_checkpoint_manager
    mock_cm.get_last_uid.return_value = 0
    mock_cm.get_uids_to_retry.return_value = []
    mock_cm.get_failed_uids_with_counts.return_value = {}

    mocked_imap_client_interface.search_all_uids_since.return_value = uids_from_imap
    mocked_imap_client_interface.fetch.return_value = ('OK', [(b'RFC822.HEADER', b"Subject: Commit Test\r\n"), b')'])
    
    mocked_db_interface.log_sync_start.return_value = 1002

    try:
        with patch('sync.EMAILS_PER_COMMIT', 5) as mock_chunk_size_for_commit, \
             patch('sync.parse_email_date', return_value="commit-date"), \
             patch('sync.decode_field', side_effect=lambda x: x if x is not None else ''):
            
            num_uids = 12 # More than 2*EMAILS_PER_COMMIT (5) and enough for checkpoint save interval
            uids_from_imap_short = [str(i) for i in range(1, num_uids + 1)]
            mocked_imap_client_interface.search_all_uids_since.return_value = uids_from_imap_short
            
            async def mock_update_progress_side_effect(uid_param, current_save_count_override=None):
                mock_cm.last_uid = int(uid_param)
                if int(uid_param) % 3 == 0:
                    await mock_cm.save_state()
            mock_cm.update_progress.side_effect = mock_update_progress_side_effect

            syncer = EmailSyncer(
                db_manager=mocked_db_interface,
                imap_client=mocked_imap_client_interface,
                mode=mode,
                mailbox=mailbox
            )

            # Prepare arguments for syncer.run()
            uids_to_fetch_tuples_short = [(uid, mailbox) for uid in uids_from_imap_short]
            total_initial_count_short = len(uids_from_imap_short)
            fetch_mode_desc_short = f"{mode} for {mailbox}"
            
            await syncer.run(uids_to_fetch_tuples_short, total_initial_count_short, fetch_mode_desc_short)

            assert mocked_db_interface.commit_with_retry.call_count == 4
            # The test's update_progress mock calls save_state for UIDs 3,6,9,12 (4 times)
            # EmailSyncer.run also calls save_state at the end of each processed chunk (3 times for 12 UIDs, EMAILS_PER_COMMIT=5)
            assert mock_cm.save_state.call_count == 7 # 4 (from update_progress) + 3 (from syncer's end-of-chunk saves)
            assert mocked_db_interface.save_email_header.call_count == num_uids

    finally:
        pass

@pytest.mark.asyncio
async def test_sync_email_headers_orchestrator_no_uids(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test sync_email_headers when IMAP returns no UIDs and no retry UIDs."""
    mailbox = "EMPTY_MAILBOX_HEADERS"
    
    mock_cm = patched_checkpoint_manager
    mock_cm.get_last_uid.return_value = 1000 # Some last UID
    mock_cm.get_uids_to_retry.return_value = [] # No UIDs to retry
    mock_cm.get_failed_uids_with_counts.return_value = {} # For syncer init

    mocked_imap_client_interface.search_all_uids_since.return_value = [] # IMAP returns no new UIDs
    mocked_imap_client_interface.select_mailbox.return_value = True

    await sync_email_headers(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mailbox=mailbox
    )

    mocked_db_interface.log_sync_start.assert_called_once()
    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True)
    
    mock_cm.get_last_uid.assert_called_once()
    mock_cm.get_uids_to_retry.assert_called_once()
    mock_cm.mark_start.assert_called_once()

    mocked_imap_client_interface.search_all.assert_called_once_with() # Called for non-Gmail special mailboxes
    
    mocked_imap_client_interface.fetch.assert_not_called()
    mocked_db_interface.save_email_header.assert_not_called()
    mock_cm.update_progress.assert_not_called()
    
    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    pos_args, kw_args = call_args_list[0]
    assert pos_args[1] == "COMPLETED_NO_NEW_HEADERS"
    assert pos_args[2] == f'No emails found on server or to retry in {mailbox}'

@pytest.mark.asyncio
async def test_sync_email_headers_imap_search_fails(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test sync_email_headers when IMAP search_all_uids_since fails."""
    mailbox = "SEARCH_FAIL_MAILBOX"
    
    mock_cm = patched_checkpoint_manager
    mock_cm.get_last_uid.return_value = 50
    mock_cm.get_uids_to_retry.return_value = []

    # mocked_imap_client_interface.search_all_uids_since.return_value = None 
    # Instead, make search_all (which is actually called) raise an error
    mocked_imap_client_interface.search_all = AsyncMock(side_effect=RuntimeError("Simulated IMAP search error"))

    await sync_email_headers(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mailbox=mailbox
    )

    mocked_db_interface.log_sync_start.assert_called_once()
    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True)
    mock_cm.mark_start.assert_called_once()
    mocked_imap_client_interface.search_all.assert_called_once_with() # Called for non-Gmail special mailboxes
    
    mocked_imap_client_interface.fetch.assert_not_called()
    mocked_db_interface.save_email_header.assert_not_called()
    
    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    pos_args, kw_args = call_args_list[0]
    assert pos_args[1] == "ERROR"
    assert "Simulated IMAP search error" in pos_args[2]

@pytest.mark.asyncio
async def test_sync_full_emails_orchestrator_no_uids(mocked_db_interface, mocked_imap_client_interface, patched_checkpoint_manager):
    """Test sync_full_emails when DB returns no UIDs for full sync and no retry UIDs."""
    mailbox = "EMPTY_MAILBOX_FULL"
    
    mock_cm = patched_checkpoint_manager
    mock_cm.get_uids_to_retry.return_value = [] # No UIDs to retry from checkpoint
    mock_cm.get_failed_uids_with_counts.return_value = {} # For syncer init

    # mocked_db_interface.get_uids_for_full_sync.return_value = [] # DB returns no UIDs needing full sync
    # Instead, mock the actual methods called:
    mocked_db_interface.get_all_header_uids_for_mailbox = AsyncMock(return_value=[]) # Simulate no headers found
    mocked_db_interface.get_synced_full_email_uids = AsyncMock(return_value=[]) # Should not be called if above is empty

    mocked_imap_client_interface.select_mailbox.return_value = True

    await sync_full_emails(
        db_manager=mocked_db_interface,
        imap_client=mocked_imap_client_interface,
        mailbox=mailbox
    )

    mocked_db_interface.log_sync_start.assert_called_once()
    mocked_imap_client_interface.select_mailbox.assert_called_once_with(mailbox, read_only=True) # Full sync is also read-only for select
    
    mock_cm.get_uids_to_retry.assert_not_called() # Not called if get_all_header_uids_for_mailbox returns empty
    mock_cm.mark_start.assert_called_once()

    # mocked_db_interface.get_uids_for_full_sync.assert_called_once_with(mailbox)
    mocked_db_interface.get_all_header_uids_for_mailbox.assert_called_once_with(mailbox)
    mocked_db_interface.get_synced_full_email_uids.assert_not_called() # Because get_all_header_uids returned []
    
    mocked_imap_client_interface.fetch.assert_not_called()
    mocked_db_interface.save_full_email.assert_not_called()
    mock_cm.update_progress.assert_not_called()
    
    mock_cm.mark_complete.assert_called_once()
    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    pos_args, kw_args = call_args_list[0]
    assert pos_args[1] == "SKIPPED" # Changed from COMPLETED_NO_EMAILS_FOR_FULL_SYNC
    assert f'No headers in DB for {mailbox}' in pos_args[2] # Updated message

@pytest.mark.asyncio
async def test_sync_attachments_orchestrator(mocked_db_interface, patched_checkpoint_manager):
    """Test basic run of sync_attachments orchestrator. (Further details depend on its impl)"""
    mailbox = "ATTACHMENTS_MAILBOX"
    
    mock_cm = patched_checkpoint_manager
    mock_cm.get_uids_to_retry.return_value = [] 
    
    mocked_db_interface.get_uids_needing_attachment_processing = AsyncMock(return_value=[]) # Example

    await sync_attachments(
        db_manager=mocked_db_interface,
        mailbox=mailbox
    )

    mocked_db_interface.log_sync_start.assert_called_once()
    mock_cm.mark_start.assert_called_once()

    mocked_db_interface.log_sync_end.assert_called_once()
    call_args_list = mocked_db_interface.log_sync_end.call_args_list
    assert len(call_args_list) == 1
    args, kwargs = call_args_list[0]
    assert args[1] == "COMPLETED" # Status is the second positional argument