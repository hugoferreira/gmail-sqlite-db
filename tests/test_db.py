# tests/test_db.py

import pytest
# We will import functions from your db.py module later
# For now, let's assume there's a way to get a connection
# and perhaps a function to initialize the schema.

# Mark the test as asyncio to work with async fixtures and await
@pytest.mark.asyncio
async def test_database_connection(db_manager):
    """Test that we can establish a connection to the test database."""
    assert db_manager.db is not None
    cursor = await db_manager.db.cursor()
    assert cursor is not None
    await cursor.execute("SELECT 1")
    result = await cursor.fetchone()
    assert result[0] == 1
    await cursor.close()

@pytest.mark.asyncio
async def test_checkpoint_state_save_and_load(db_manager):
    """Test saving and loading checkpoint state."""
    mode = "full_sync"
    mailbox = "INBOX"
    last_uid = 100
    in_progress = False
    timestamp = "2023-10-26T12:00:00Z"

    # Initial load should be None or default
    initial_state = await db_manager.load_checkpoint_state(mode, mailbox)
    # The load_checkpoint_state method in db.py seems to return None if no record or provides defaults.
    # Let's assume for a new state, it might return None or a dict with default values.
    # If it returns defaults, we might need to adjust the assertion.
    # Based on current db.py, it returns a dict with last_uid=0 if no state, or the state.
    if initial_state:
         assert initial_state.get("last_uid", 0) == 0 # Check default or actual value
    # else, it might be None, which is also a valid initial state for some interpretations.

    await db_manager.save_checkpoint_state(mode, mailbox, last_uid, in_progress, timestamp)
    
    loaded_state = await db_manager.load_checkpoint_state(mode, mailbox)
    assert loaded_state is not None
    # Mode and mailbox are implicit from the query, not returned in the dict by load_checkpoint_state
    # assert loaded_state["mode"] == mode
    # assert loaded_state["mailbox"] == mailbox 
    assert loaded_state["last_uid"] == last_uid
    assert loaded_state["in_progress"] == in_progress # in_progress is already bool after load
    assert loaded_state["timestamp"] == timestamp

    # Test update
    new_last_uid = 150
    new_in_progress = True
    new_timestamp = "2023-10-26T13:00:00Z"
    await db_manager.save_checkpoint_state(mode, mailbox, new_last_uid, new_in_progress, new_timestamp)

    updated_state = await db_manager.load_checkpoint_state(mode, mailbox)
    assert updated_state is not None
    assert updated_state["last_uid"] == new_last_uid
    assert updated_state["in_progress"] == new_in_progress # in_progress is already bool after load
    assert updated_state["timestamp"] == new_timestamp

@pytest.mark.asyncio
async def test_checkpoint_failed_uids_management(db_manager):
    """Test adding, retrieving, and removing checkpoint failed UIDs."""
    mode = "headers_sync"
    mailbox = "IMPORTANT"
    uid1 = "failed_uid_1"
    uid2 = "failed_uid_2"

    # Initial state: no failed UIDs
    initial_failed_uids = await db_manager.get_checkpoint_failed_uids(mode, mailbox)
    assert initial_failed_uids == {}

    # Add one failed UID
    await db_manager.add_or_update_checkpoint_failed_uid(mode, mailbox, uid1, retry_count=1)
    failed_uids_after_add1 = await db_manager.get_checkpoint_failed_uids(mode, mailbox)
    assert failed_uids_after_add1 == {uid1: 1}

    # Add another failed UID and update retry count for the first one
    await db_manager.add_or_update_checkpoint_failed_uid(mode, mailbox, uid1, retry_count=2)
    await db_manager.add_or_update_checkpoint_failed_uid(mode, mailbox, uid2, retry_count=1)
    failed_uids_after_add2 = await db_manager.get_checkpoint_failed_uids(mode, mailbox)
    assert failed_uids_after_add2 == {uid1: 2, uid2: 1}

    # Remove one failed UID
    await db_manager.remove_checkpoint_failed_uid(mode, mailbox, uid1)
    failed_uids_after_remove = await db_manager.get_checkpoint_failed_uids(mode, mailbox)
    assert failed_uids_after_remove == {uid2: 1}

    # Remove the last one
    await db_manager.remove_checkpoint_failed_uid(mode, mailbox, uid2)
    final_failed_uids = await db_manager.get_checkpoint_failed_uids(mode, mailbox)
    assert final_failed_uids == {}

# --- Property-Based Tests with Hypothesis ---
from hypothesis import given, settings
from hypothesis.strategies import text, integers, composite, dates, times, datetimes, booleans, binary, emails as hypothesis_emails
from hypothesis import HealthCheck
import datetime

# Define a strategy for generating plausible UIDs (avoiding overly complex strings for now)
# SQLite TEXT can handle a lot, but let's keep it reasonable for test data.
# UIDs in email are often numeric or simple alphanumeric.
plausible_uids = text(alphabet='0123456789abcdefghijklmnopqrstuvwxyz', min_size=5, max_size=20)

# Strategy for generating email header data
@composite
def email_header_data_strategy(draw):
    uid = draw(plausible_uids)
    msg_from = draw(hypothesis_emails())
    msg_to = draw(hypothesis_emails())
    msg_cc = draw(hypothesis_emails())
    subject = draw(text(min_size=0, max_size=255).filter(lambda x: "\x00" not in x))
    
    # Generate naive datetimes for the strategy
    naive_dt = draw(datetimes(
        min_value=datetime.datetime(1970, 1, 1, 0, 0, 0),  # Naive
        max_value=datetime.datetime(2038, 1, 1, 0, 0, 0)   # Naive
    ))
    # Make it UTC aware before formatting, to include Z or offset
    aware_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
    msg_date = aware_dt.isoformat()
    
    mailbox = draw(text(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', min_size=3, max_size=15))
    return uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox

@pytest.mark.asyncio
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(header_data=email_header_data_strategy())
async def test_property_save_and_retrieve_email_header(db_manager, header_data):
    """Property: Data saved via save_email_header can be retrieved accurately."""
    uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox = header_data

    await db_manager.save_email_header(uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox)

    async with db_manager.db.execute(
        "SELECT uid, msg_from, msg_to, msg_cc, subject, msg_date, mailbox FROM emails WHERE uid = ?", (uid,)
    ) as cursor:
        row = await cursor.fetchone()
    
    assert row is not None, f"Failed to retrieve header for UID: {uid}"
    ret_uid, ret_from, ret_to, ret_cc, ret_subject, ret_date, ret_mailbox = row

    assert ret_uid == uid
    assert ret_from == msg_from
    assert ret_to == msg_to
    assert ret_cc == msg_cc
    assert ret_subject == subject
    assert ret_date == msg_date
    assert ret_mailbox == mailbox

# Strategy for generating full email data
@composite
def full_email_data_strategy(draw):
    uid = draw(plausible_uids)
    mailbox = draw(text(alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', min_size=3, max_size=15))
    raw_email = draw(binary(min_size=10, max_size=1024))
    
    # Generate naive datetimes for the strategy
    naive_dt = draw(datetimes(
        min_value=datetime.datetime(1970, 1, 1, 0, 0, 0),  # Naive
        max_value=datetime.datetime(2038, 1, 1, 0, 0, 0)   # Naive
    ))
    # Make it UTC aware before formatting
    aware_dt = naive_dt.replace(tzinfo=datetime.timezone.utc)
    fetched_at = aware_dt.isoformat()
    return uid, mailbox, raw_email, fetched_at

@pytest.mark.asyncio
@settings(max_examples=30, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(email_data=full_email_data_strategy())
async def test_property_save_and_retrieve_full_email(db_manager, email_data):
    """Property: Data saved via save_full_email can be retrieved accurately."""
    uid, mailbox, raw_email, fetched_at = email_data

    await db_manager.save_full_email(uid, mailbox, raw_email, fetched_at)

    retrieved_content = await db_manager.get_raw_email_content(uid, mailbox)
    assert retrieved_content is not None, f"Failed to retrieve full email for UID: {uid} in {mailbox}"
    assert retrieved_content == raw_email

    async with db_manager.db.execute(
        "SELECT fetched_at FROM full_emails WHERE uid = ? AND mailbox = ?", (uid, mailbox)
    ) as cursor:
        row = await cursor.fetchone()
    assert row is not None, f"Failed to retrieve fetched_at for UID: {uid} in {mailbox}"
    assert row[0] == fetched_at