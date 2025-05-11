# tests/test_checkpoint.py

import pytest
import pytest_asyncio # For async fixtures if needed, though main mock might be sync
from unittest.mock import MagicMock, AsyncMock # AsyncMock for async db methods
import datetime
from unittest.mock import patch

# Assuming CheckpointManager is in checkpoint.py in the root
from checkpoint import CheckpointManager
from config import MAX_UID_FETCH_RETRIES # Import for MAX_RETRIES

# Define a constant for the test, reflecting the logic in CheckpointManager.update_progress
TEST_SAVE_INTERVAL = 250 

@pytest.mark.asyncio
async def test_checkpoint_manager_initialization(mocked_db_interface):
    """Test basic initialization and that state loading is attempted."""
    mode = "headers"
    mailbox = "INBOX"
    
    initial_db_state = {
        'last_uid': 10,
        'failed_uids': {'uid1': 1},
        'in_progress': False,
        'timestamp': datetime.datetime.now().isoformat()
    }
    mocked_db_interface.load_checkpoint_state.return_value = initial_db_state
    
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    
    last_uid = await manager.get_last_uid()
    
    mocked_db_interface.load_checkpoint_state.assert_called_once_with(mode, mailbox)
    assert manager.mode == mode
    assert manager.mailbox == mailbox
    assert last_uid == 10
    assert manager.last_uid == 10
    assert manager.failed_uids == {'uid1': 1}
    assert manager.in_progress is False
    assert manager.timestamp == initial_db_state['timestamp']

@pytest.mark.asyncio
async def test_checkpoint_manager_initialization_no_db_state(mocked_db_interface):
    """Test initialization when no state exists in the DB."""
    mode = "full"
    mailbox = "SENT"
    
    mocked_db_interface.load_checkpoint_state.return_value = None
    
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    
    last_uid = await manager.get_last_uid()
    failed_uids = await manager.get_failed_uids_with_counts()
    interrupted = await manager.was_interrupted()
    
    mocked_db_interface.load_checkpoint_state.assert_called_once_with(mode, mailbox)
    assert last_uid == 0
    assert manager.last_uid == 0
    assert failed_uids == {}
    assert manager.failed_uids == {}
    assert interrupted is False
    assert manager.in_progress is False
    assert manager.timestamp is None

@pytest.mark.asyncio
async def test_save_state(mocked_db_interface, mock_checkpoint_datetime):
    mode = "headers"
    mailbox = "INBOX"
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    manager.last_uid = 50
    manager.in_progress = True

    expected_iso_timestamp = mock_checkpoint_datetime.datetime.now.return_value.isoformat()

    await manager.save_state()

    mocked_db_interface.save_checkpoint_state.assert_called_once_with(
        mode=mode,
        mailbox=mailbox,
        last_uid=50,
        in_progress=True,
        timestamp=expected_iso_timestamp
    )
    assert manager.timestamp == expected_iso_timestamp

@pytest.mark.asyncio
async def test_set_mailbox(mocked_db_interface):
    mode = "full"
    initial_mailbox = "INBOX"
    new_mailbox = "SENT"

    inbox_state = {'last_uid': 100, 'failed_uids': {}, 'in_progress': False, 'timestamp': 't1'}
    sent_state = {'last_uid': 20, 'failed_uids': {'s_uid1': 1}, 'in_progress': True, 'timestamp': 't2'}
    
    async def mock_load_state(called_mode, called_mailbox):
        if called_mailbox == initial_mailbox:
            return inbox_state
        elif called_mailbox == new_mailbox:
            return sent_state
        return None
    mocked_db_interface.load_checkpoint_state.side_effect = mock_load_state
    
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=initial_mailbox)
    await manager.get_last_uid() 
    assert manager.last_uid == 100
    mocked_db_interface.load_checkpoint_state.assert_any_call(mode, initial_mailbox)
    
    await manager.set_mailbox(new_mailbox)
    
    mocked_db_interface.load_checkpoint_state.assert_any_call(mode, new_mailbox)
    assert manager.mailbox == new_mailbox
    assert manager.last_uid == 20
    assert manager.failed_uids == {'s_uid1': 1}
    assert manager.in_progress is True
    assert mocked_db_interface.load_checkpoint_state.call_count == 2

@pytest.mark.asyncio
async def test_mark_start_and_complete(mocked_db_interface, mock_checkpoint_datetime):
    mode = "headers"
    mailbox = "TESTBOX"
    
    expected_ts_start = mock_checkpoint_datetime.datetime.now.return_value.isoformat()
    state_after_mark_start_save = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': True, 'timestamp': expected_ts_start
    }

    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': False, 'timestamp': None
    }
    
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)

    await manager.mark_start()
    assert manager.in_progress is True
    assert mocked_db_interface.save_checkpoint_state.call_count == 1
    saved_args_start = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert saved_args_start['in_progress'] is True
    assert saved_args_start['timestamp'] == expected_ts_start
    assert manager.timestamp == expected_ts_start

    mocked_db_interface.load_checkpoint_state.return_value = state_after_mark_start_save

    # For mark_complete, set up a new mock datetime object
    fixed_complete_datetime = datetime.datetime(2023, 1, 2, 10, 30, 0, tzinfo=datetime.timezone.utc) # A different time
    expected_ts_complete = fixed_complete_datetime.isoformat()
    mock_checkpoint_datetime.datetime.now.return_value = fixed_complete_datetime

    await manager.mark_complete()
    assert manager.in_progress is False
    assert mocked_db_interface.save_checkpoint_state.call_count == 2
    saved_args_complete = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert saved_args_complete['in_progress'] is False
    assert saved_args_complete['timestamp'] == expected_ts_complete
    assert manager.timestamp == expected_ts_complete

    assert mocked_db_interface.load_checkpoint_state.call_count == 1 

@pytest.mark.asyncio
async def test_update_progress(mocked_db_interface, mock_checkpoint_datetime):
    mode = "full"
    mailbox = "SPAM"
    save_interval = TEST_SAVE_INTERVAL # Use the test constant

    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': True, 'timestamp': 't_initial'
    }
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    await manager._ensure_state_loaded() 
    mocked_db_interface.save_checkpoint_state.reset_mock()

    expected_ts_update = mock_checkpoint_datetime.datetime.now.return_value.isoformat()

    await manager.update_progress("50")
    assert manager.last_uid == 50
    # current_save_count doesn't exist. Check save_checkpoint_state call count directly.
    # 50 % 250 != 0, so save_state should not be called.
    mocked_db_interface.save_checkpoint_state.assert_not_called()

    await manager.update_progress(str(save_interval)) # uid = 250, save_interval = 250
    assert manager.last_uid == save_interval
    # 250 % 250 == 0, so save_state should be called once now.
    mocked_db_interface.save_checkpoint_state.assert_called_once()
    args_save1 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save1['last_uid'] == save_interval
    assert args_save1['timestamp'] == expected_ts_update # Assuming datetime mock isn't advanced
    assert manager.timestamp == expected_ts_update

    mocked_db_interface.save_checkpoint_state.reset_mock() # Reset for next check

    await manager.update_progress(str(save_interval + 1)) # uid = 251
    assert manager.last_uid == save_interval + 1
    # 251 % 250 != 0, save_state should not be called again.
    mocked_db_interface.save_checkpoint_state.assert_not_called()

    await manager.update_progress(str(save_interval * 2)) # uid = 500
    assert manager.last_uid == save_interval * 2
    # 500 % 250 == 0, save_state should be called again.
    mocked_db_interface.save_checkpoint_state.assert_called_once()
    args_save2 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save2['last_uid'] == save_interval * 2
    # Ensure timestamp is updated if datetime mock is consistent
    # If datetime mock is reset or provides new values, this might need adjustment
    assert args_save2['timestamp'] == expected_ts_update 
    assert manager.timestamp == expected_ts_update

@pytest.mark.asyncio
async def test_failed_uid_management(mocked_db_interface, mock_checkpoint_datetime):
    mode = "full"
    mailbox = "TRASH"
    uid1 = "1001"
    uid2 = "1002"

    initial_timestamp = "t0_initial_from_load"
    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 1000, 'failed_uids': {}, 'in_progress': False, 'timestamp': initial_timestamp
    }
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    await manager._ensure_state_loaded()
    assert await manager.get_failed_uids_with_counts() == {}
    assert manager.timestamp == initial_timestamp
    
    mocked_db_interface.save_checkpoint_state.reset_mock()

    expected_ts_failed_uid_ops = mock_checkpoint_datetime.datetime.now.return_value.isoformat()

    await manager.add_failed_uid(uid1)
    assert await manager.get_failed_uids_with_counts() == {uid1: 1}
    mocked_db_interface.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid1, retry_count=1
    )
    assert mocked_db_interface.save_checkpoint_state.call_count == 1 
    args_save1 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save1['timestamp'] == expected_ts_failed_uid_ops
    assert manager.timestamp == expected_ts_failed_uid_ops

    await manager.add_failed_uid(uid1)
    assert await manager.get_failed_uids_with_counts() == {uid1: 2}
    mocked_db_interface.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid1, retry_count=2
    )
    assert mocked_db_interface.save_checkpoint_state.call_count == 2
    args_save2 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save2['timestamp'] == expected_ts_failed_uid_ops
    assert manager.timestamp == expected_ts_failed_uid_ops

    await manager.add_failed_uid(uid2)
    assert await manager.get_failed_uids_with_counts() == {uid1: 2, uid2: 1}
    mocked_db_interface.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid2, retry_count=1
    )
    assert mocked_db_interface.save_checkpoint_state.call_count == 3
    args_save3 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save3['timestamp'] == expected_ts_failed_uid_ops
    assert manager.timestamp == expected_ts_failed_uid_ops

    await manager.clear_failed_uid(uid1)
    assert await manager.get_failed_uids_with_counts() == {uid2: 1}
    mocked_db_interface.remove_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid1
    )
    assert mocked_db_interface.save_checkpoint_state.call_count == 4
    args_save4 = mocked_db_interface.save_checkpoint_state.call_args[1]
    assert args_save4['timestamp'] == expected_ts_failed_uid_ops
    assert manager.timestamp == expected_ts_failed_uid_ops

@pytest.mark.asyncio
async def test_get_uids_to_retry_max_retries(mocked_db_interface):
    mode = "headers"
    mailbox = "RETRY_LIMIT_BOX"
    
    # Use the imported MAX_UID_FETCH_RETRIES
    current_max_retries = MAX_UID_FETCH_RETRIES 

    failed_uids_from_db = {
        "uid_ok1": 1,
        "uid_too_many_retries1": current_max_retries, 
        "uid_ok2": current_max_retries - 1,
        "uid_way_over_retries": current_max_retries + 5
    }
    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': failed_uids_from_db, 'in_progress': False, 'timestamp': 't_retry'
    }
    
    manager = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    await manager._ensure_state_loaded()
    
    uids_to_retry = await manager.get_uids_to_retry(current_max_retries) # Pass the value
    permanently_failed = await manager.get_permanently_failed_uids(current_max_retries) # Pass the value

    assert sorted(uids_to_retry) == sorted(["uid_ok1", "uid_ok2"])
    assert sorted(permanently_failed) == sorted(["uid_too_many_retries1", "uid_way_over_retries"])

@pytest.mark.asyncio
async def test_was_interrupted(mocked_db_interface):
    mode = "full"
    mailbox = "INTERRUPT_TEST"

    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 10, 'failed_uids': {}, 'in_progress': False, 'timestamp': 't1'
    }
    manager1 = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    assert await manager1.was_interrupted() is False

    mocked_db_interface.load_checkpoint_state.return_value = {
        'last_uid': 20, 'failed_uids': {}, 'in_progress': True, 'timestamp': 't2'
    }
    manager2 = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    assert await manager2.was_interrupted() is True
    
    mocked_db_interface.load_checkpoint_state.return_value = None
    manager3 = CheckpointManager(db_manager=mocked_db_interface, mode=mode, mailbox=mailbox)
    assert await manager3.was_interrupted() is False 