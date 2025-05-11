# tests/test_checkpoint.py

import pytest
import pytest_asyncio # For async fixtures if needed, though main mock might be sync
from unittest.mock import MagicMock, AsyncMock # AsyncMock for async db methods
import datetime
from unittest.mock import patch

# Assuming CheckpointManager is in checkpoint.py in the root
from checkpoint import CheckpointManager

@pytest.fixture
def mock_db_manager():
    """Provides a MagicMock for the DatabaseManager."""
    db_mock = MagicMock()
    # Configure async methods on the mock if they are directly awaited by CheckpointManager
    # For example, load_checkpoint_state is async and returns a dict or None
    db_mock.load_checkpoint_state = AsyncMock(return_value=None) 
    db_mock.save_checkpoint_state = AsyncMock()
    db_mock.add_or_update_checkpoint_failed_uid = AsyncMock()
    db_mock.remove_checkpoint_failed_uid = AsyncMock()
    # get_checkpoint_failed_uids is called by load_checkpoint_state, 
    # so its return value is part of load_checkpoint_state mock setup usually.
    # However, if load_checkpoint_state returns a structure that CheckpointManager then uses
    # to call get_checkpoint_failed_uids separately (which it doesn't seem to do based on code), 
    # then it would need separate mocking. The current db.py load_checkpoint_state fetches them internally.
    return db_mock

# Example placeholder test
@pytest.mark.asyncio
async def test_checkpoint_manager_initialization(mock_db_manager):
    """Test basic initialization and that state loading is attempted."""
    mode = "headers"
    mailbox = "INBOX"
    
    # Simulate load_checkpoint_state returning some initial data
    initial_db_state = {
        'last_uid': 10,
        'failed_uids': {'uid1': 1}, # This is what load_checkpoint_state should return now
        'in_progress': False,
        'timestamp': datetime.datetime.now().isoformat()
    }
    mock_db_manager.load_checkpoint_state.return_value = initial_db_state
    
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)
    
    # Accessing a property like get_last_uid should trigger _ensure_state_loaded
    last_uid = await manager.get_last_uid()
    
    mock_db_manager.load_checkpoint_state.assert_called_once_with(mode, mailbox)
    assert manager.mode == mode
    assert manager.mailbox == mailbox
    assert last_uid == 10
    assert manager.last_uid == 10
    assert manager.failed_uids == {'uid1': 1}
    assert manager.in_progress is False
    assert manager.timestamp == initial_db_state['timestamp']

@pytest.mark.asyncio
async def test_checkpoint_manager_initialization_no_db_state(mock_db_manager):
    """Test initialization when no state exists in the DB."""
    mode = "full"
    mailbox = "SENT"
    
    mock_db_manager.load_checkpoint_state.return_value = None # Simulate no state in DB
    
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)
    
    # Accessing a property to trigger load
    last_uid = await manager.get_last_uid()
    failed_uids = await manager.get_failed_uids_with_counts()
    interrupted = await manager.was_interrupted()
    
    mock_db_manager.load_checkpoint_state.assert_called_once_with(mode, mailbox)
    assert last_uid == 0
    assert manager.last_uid == 0
    assert failed_uids == {}
    assert manager.failed_uids == {}
    assert interrupted is False
    assert manager.in_progress is False
    assert manager.timestamp is None

@pytest.mark.asyncio
async def test_save_state(mock_db_manager):
    mode = "headers"
    mailbox = "INBOX"
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)
    # Prime the state as if it was loaded
    manager.last_uid = 50
    manager.in_progress = True
    # failed_uids are handled by separate db calls, save_state focuses on core state

    with patch('checkpoint.datetime') as mock_datetime:
        mock_now = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = mock_now
        iso_timestamp = mock_now.isoformat()

        await manager.save_state()

    mock_db_manager.save_checkpoint_state.assert_called_once_with(
        mode=mode,
        mailbox=mailbox,
        last_uid=50,
        in_progress=True,
        timestamp=iso_timestamp
    )
    assert manager.timestamp == iso_timestamp

@pytest.mark.asyncio
async def test_set_mailbox(mock_db_manager):
    mode = "full"
    initial_mailbox = "INBOX"
    new_mailbox = "SENT"

    # Initial state for INBOX
    inbox_state = {'last_uid': 100, 'failed_uids': {}, 'in_progress': False, 'timestamp': 't1'}
    # State for SENT (or None if it doesn't exist yet)
    sent_state = {'last_uid': 20, 'failed_uids': {'s_uid1': 1}, 'in_progress': True, 'timestamp': 't2'}
    
    # Configure load_checkpoint_state to return different states based on mailbox
    async def mock_load_state(called_mode, called_mailbox):
        if called_mailbox == initial_mailbox:
            return inbox_state
        elif called_mailbox == new_mailbox:
            return sent_state
        return None
    mock_db_manager.load_checkpoint_state.side_effect = mock_load_state
    
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=initial_mailbox)
    # Trigger initial load for INBOX
    await manager.get_last_uid() 
    assert manager.last_uid == 100
    mock_db_manager.load_checkpoint_state.assert_called_with(mode, initial_mailbox)
    
    # Set new mailbox
    await manager.set_mailbox(new_mailbox)
    
    # Check that state for SENT was loaded
    mock_db_manager.load_checkpoint_state.assert_called_with(mode, new_mailbox)
    assert manager.mailbox == new_mailbox
    assert manager.last_uid == 20
    assert manager.failed_uids == {'s_uid1': 1}
    assert manager.in_progress is True
    # Ensure call count is 2 (initial load, then load after set_mailbox)
    assert mock_db_manager.load_checkpoint_state.call_count == 2

@pytest.mark.asyncio
async def test_mark_start_and_complete(mock_db_manager):
    mode = "headers"
    mailbox = "TESTBOX"
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)
    
    # Mock load_checkpoint_state to return a default-like state initially
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': False, 'timestamp': None
    }

    # Mark start
    await manager.mark_start()
    assert manager.in_progress is True
    # save_state is called, which then calls db_manager.save_checkpoint_state
    # First call to load, first call to save
    assert mock_db_manager.save_checkpoint_state.call_count == 1
    saved_args_start = mock_db_manager.save_checkpoint_state.call_args[1]
    assert saved_args_start['in_progress'] is True

    # Mark complete
    # Reset load for the _ensure_state_loaded in mark_complete if it matters for subsequent checks
    # For this test, we assume the state from mark_start is implicitly carried if not re-loaded
    # or that _ensure_state_loaded in mark_complete just re-affirms from memory or db mock.
    # The fixture re-mocking for load_checkpoint_state happens per test, not per call within a test.
    # Let's assume it loads the state that was just saved by mark_start (in_progress=True)
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': True, # Reflects state after mark_start
        'timestamp': saved_args_start['timestamp']
    }
    
    await manager.mark_complete()
    assert manager.in_progress is False
    # Second call to save
    assert mock_db_manager.save_checkpoint_state.call_count == 2
    saved_args_complete = mock_db_manager.save_checkpoint_state.call_args[1]
    assert saved_args_complete['in_progress'] is False
    # Ensure load_checkpoint_state was called only by the first public method (mark_start)
    # as the state is now sticky until mailbox changes.
    assert mock_db_manager.load_checkpoint_state.call_count == 1

@pytest.mark.asyncio
async def test_update_progress(mock_db_manager):
    mode = "full"
    mailbox = "SPAM"
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 0, 'failed_uids': {}, 'in_progress': True, 'timestamp': 't_initial'
    }
    await manager._ensure_state_loaded() # Manually load initial state for this test structure

    # 1. Update with a UID larger than initial 0 - should update last_uid
    await manager.update_progress("50")
    assert manager.last_uid == 50 # last_uid should now be 50
    mock_db_manager.save_checkpoint_state.assert_not_called()

    # 2. Update with a smaller UID - should not change last_uid or save
    await manager.update_progress("30") 
    assert manager.last_uid == 50 # Still 50
    mock_db_manager.save_checkpoint_state.assert_not_called()

    # 3. Update with a larger UID, not on save interval
    await manager.update_progress("100")
    assert manager.last_uid == 100
    mock_db_manager.save_checkpoint_state.assert_not_called()

    # 4. Update with a UID that triggers save (e.g., % 250 == 0)
    await manager.update_progress("250")
    assert manager.last_uid == 250
    assert mock_db_manager.save_checkpoint_state.call_count == 1
    saved_args = mock_db_manager.save_checkpoint_state.call_args[1]
    assert saved_args['last_uid'] == 250

    # 5. Update again, past save interval, no new save
    await manager.update_progress("251")
    assert manager.last_uid == 251
    assert mock_db_manager.save_checkpoint_state.call_count == 1 # Still 1

@pytest.mark.asyncio
async def test_failed_uid_management(mock_db_manager):
    mode = "full"
    mailbox = "TRASH"
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)

    # Initial state: no failed UIDs
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 1000, 'failed_uids': {}, 'in_progress': False, 'timestamp': 't0'
    }
    await manager._ensure_state_loaded()
    assert await manager.get_failed_uids_with_counts() == {}

    # Add a failed UID
    uid1 = "1001"
    await manager.add_failed_uid(uid1)
    assert await manager.get_failed_uids_with_counts() == {uid1: 1}
    mock_db_manager.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid1, retry_count=1
    )
    # save_state is also called by add_failed_uid
    assert mock_db_manager.save_checkpoint_state.call_count == 1 

    # Add same UID again (retry count should increment)
    # We need to update what load_checkpoint_state would return for failed_uids for next _ensure_state_loaded
    # or ensure internal state is what we expect before the second add.
    # The current add_failed_uid updates internal state then DB.
    await manager.add_failed_uid(uid1)
    assert await manager.get_failed_uids_with_counts() == {uid1: 2}
    mock_db_manager.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid1, retry_count=2
    )
    assert mock_db_manager.save_checkpoint_state.call_count == 2

    # Add another failed UID
    uid2 = "1002"
    await manager.add_failed_uid(uid2)
    assert await manager.get_failed_uids_with_counts() == {uid1: 2, uid2: 1}
    mock_db_manager.add_or_update_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid2, retry_count=1
    )
    assert mock_db_manager.save_checkpoint_state.call_count == 3

    # Test get_uids_to_retry and get_permanently_failed_uids
    max_r = 2
    assert await manager.get_uids_to_retry(max_retries=max_r) == [uid2] # uid1 has 2 retries, uid2 has 1
    # If uid1 has 2 (==max_r), it should not be in retry. Let's adjust expectation.
    # Retry if count < max_retries. So if max_retries is 2, uid1 (count 2) shouldn't be retried.
    # uid2 (count 1) should be.
    # Corrected: get_uids_to_retry should return uids with count < max_retries
    assert sorted(await manager.get_uids_to_retry(max_retries=max_r)) == sorted([uid2])
    assert sorted(await manager.get_permanently_failed_uids(max_retries=max_r)) == sorted([uid1])

    # Clear a failed UID
    await manager.clear_failed_uid(uid2)
    assert await manager.get_failed_uids_with_counts() == {uid1: 2}
    mock_db_manager.remove_checkpoint_failed_uid.assert_called_with(
        mode=mode, mailbox=mailbox, uid=uid2
    )
    assert mock_db_manager.save_checkpoint_state.call_count == 4 # save_state called by clear_failed_uid

    # Clear a non-existent failed UID (should not error, no DB call for remove)
    # Reset call count for remove_checkpoint_failed_uid for this specific sub-test
    mock_db_manager.remove_checkpoint_failed_uid.reset_mock()
    await manager.clear_failed_uid("9999") # Non-existent
    mock_db_manager.remove_checkpoint_failed_uid.assert_not_called()
    # save_state is NOT called by clear_failed_uid if the uid is not in self.failed_uids.
    # The call count should remain unchanged from the previous step.
    assert mock_db_manager.save_checkpoint_state.call_count == 4 # Corrected expectation

@pytest.mark.asyncio
async def test_was_interrupted(mock_db_manager):
    mode = "headers"
    mailbox = "ARCHIVE"
    manager = CheckpointManager(db_manager=mock_db_manager, mode=mode, mailbox=mailbox)

    # Scenario 1: Not interrupted
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 10, 'failed_uids': {}, 'in_progress': False, 'timestamp': 't1'
    }
    assert await manager.was_interrupted() is False

    # Scenario 2: Interrupted
    # Set the mock return value for Scenario 2 *before* forcing the reload.
    mock_db_manager.load_checkpoint_state.return_value = {
        'last_uid': 20, 'failed_uids': {}, 'in_progress': True, 'timestamp': 't2'
    }
    # Force a reload of state by "re-setting" the mailbox, which clears the load flag
    # and uses the new mock return value set above.
    await manager.set_mailbox(mailbox)

    # Reset the manager or create new one to force re-load for this scenario if state is sticky
    # For this test structure, just changing return_value of mock is enough as manager calls _ensure_state_loaded
    assert await manager.was_interrupted() is True 