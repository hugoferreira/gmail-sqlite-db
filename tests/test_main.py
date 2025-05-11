import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import sys

from db import DatabaseManager as ActualDatabaseManager
from imap_client import ImapClient as ActualImapClient
import main as main_module

@pytest.fixture
def mock_argv(monkeypatch):
    """Fixture to mock sys.argv for testing command-line argument parsing."""
    def _mock_argv(args_list):
        monkeypatch.setattr(sys, 'argv', ['main.py'] + args_list)
    return _mock_argv

@pytest.fixture
def mock_config(monkeypatch):
    """Fixture to mock the config module attributes if necessary."""
    mock_cfg = MagicMock()
    mock_cfg.DEBUG_MODE = False
    mock_cfg.DEFAULT_DB_PATH = main_module.DEFAULT_DB_PATH # Use actual default from module
    mock_cfg.DEFAULT_CREDS_JSON_PATH = main_module.DEFAULT_CREDS_JSON_PATH
    
    monkeypatch.setattr('main.config', mock_cfg)
    if hasattr(main_module, 'config'):
        monkeypatch.setattr(main_module.config, 'DEBUG_MODE', False, raising=False)
    return mock_cfg

@pytest.mark.asyncio
async def test_main_no_command(mock_argv, capsys, mock_config):
    mock_argv([])
    with pytest.raises(SystemExit) as e:
        await main_module.main()
    assert e.type == SystemExit
    assert e.value.code == 2
    captured = capsys.readouterr()
    assert "usage: main.py" in captured.err
    assert "error: the following arguments are required: command" in captured.err

@pytest.mark.asyncio
@patch('main.handle_list_mailboxes_command', new_callable=AsyncMock)
async def test_main_dispatches_list_mailboxes(mock_handler, mock_argv, mock_config):
    test_user = "test@example.com"
    test_creds_path = "fake_creds.json"
    # Global args first, then command
    mock_argv(['--user', test_user, '--creds', test_creds_path, 'list-mailboxes'])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'list-mailboxes'
    assert call_args.user == test_user
    assert call_args.creds == test_creds_path
    assert call_args.db == main_module.DEFAULT_DB_PATH
    assert call_args.host == 'imap.gmail.com'

@pytest.mark.asyncio
async def test_main_list_mailboxes_missing_user(mock_argv, capsys, mock_config):
    mock_argv(['list-mailboxes']) # No --user, error is caught by main.py's specific check
    with pytest.raises(SystemExit) as e:
        await main_module.main()
    assert e.type == SystemExit
    assert e.value.code == 2
    captured = capsys.readouterr()
    assert "error: the following arguments are required for list-mailboxes: --user" in captured.err
    
@pytest.mark.asyncio
@patch('main.handle_query_command', new_callable=AsyncMock)
async def test_main_dispatches_query_list(mock_handler, mock_argv, mock_config):
    # Global args first (like --db), then command, then command-specific args
    mock_argv(['--db', 'test_db.sqlite', 'query', '--list-queries'])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'query'
    assert call_args.list_queries is True
    assert call_args.db == 'test_db.sqlite'
    assert call_args.query_name is None

@pytest.mark.asyncio
@patch('main.handle_query_command', new_callable=AsyncMock)
async def test_main_dispatches_query_specific(mock_handler, mock_argv, mock_config):
    query_name = "find_large_emails"
    limit = 100
    start_date = "2023-01-01"
    # Global args (like --db, if specified), then command, then command-specific args
    mock_argv([
        'query', query_name,
        '--limit', str(limit),
        '--start-date', start_date
    ])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'query'
    assert call_args.query_name == query_name
    assert call_args.limit == limit
    assert call_args.start_date == start_date
    assert call_args.end_date is None
    assert call_args.list_queries is False

@pytest.mark.asyncio
@patch('main.handle_analytics_command', new_callable=AsyncMock)
async def test_main_dispatches_analytics_default(mock_handler, mock_argv, mock_config, mock_main_datetime):
    # mock_main_datetime is injected to patch 'main.datetime' used by analytics default year
    mock_argv(['analytics'])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'analytics'
    assert call_args.db == main_module.DEFAULT_DB_PATH
    # Check that the year comes from the mocked datetime
    assert call_args.year == mock_main_datetime.datetime.now.return_value.year
    assert call_args.calendar is False
    assert call_args.metric == 'emails'

@pytest.mark.asyncio
@patch('main.handle_analytics_command', new_callable=AsyncMock)
async def test_main_dispatches_analytics_custom(mock_handler, mock_argv, mock_config, mock_main_datetime):
    # mock_main_datetime is used if main.py code directly uses datetime.now() for some logic
    # that isn't overridden by CLI args. In this test, year is explicit via CLI.
    # The fixture will still patch main.datetime if it's imported there.
    test_db = "analytics_db.sqlite"
    test_year = 2022
    test_metric = "attachments"
    mock_argv([
        '--db', test_db,
        'analytics',
        '--year', str(test_year),
        '--calendar',
        '--metric', test_metric
    ])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'analytics'
    assert call_args.db == test_db
    assert call_args.year == test_year
    assert call_args.calendar is True
    assert call_args.metric == test_metric

@pytest.mark.asyncio
@patch('main.handle_sync_command', new_callable=AsyncMock)
async def test_main_dispatches_sync_headers(mock_handler, mock_argv, mock_config):
    test_user = "sync_user@example.com"
    test_mailbox = "Archive"
    mock_argv([
        '--user', test_user,
        '--db', main_module.DEFAULT_DB_PATH, 
        'sync', 'headers',
        '--mailbox', test_mailbox,
        '--all-mailboxes'
    ])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'sync'
    assert call_args.sync_mode == 'headers'
    assert call_args.user == test_user
    assert call_args.mailbox == test_mailbox
    assert call_args.all_mailboxes is True
    assert call_args.db == main_module.DEFAULT_DB_PATH

@pytest.mark.asyncio
@patch('main.handle_sync_command', new_callable=AsyncMock)
async def test_main_dispatches_sync_full(mock_handler, mock_argv, mock_config):
    test_user = "full_sync@example.com"
    mock_argv([
        '--user', test_user, 
        'sync', 'full'
    ])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'sync'
    assert call_args.sync_mode == 'full'
    assert call_args.user == test_user
    assert call_args.mailbox == 'INBOX'
    assert call_args.all_mailboxes is False

@pytest.mark.asyncio
@patch('main.handle_sync_command', new_callable=AsyncMock)
async def test_main_dispatches_sync_attachments(mock_handler, mock_argv, mock_config):
    test_user = "attach_user@example.com"
    mock_argv([
        '--user', test_user, 
        'sync', 'attachments',
        '--all-mailboxes'
    ])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'sync'
    assert call_args.sync_mode == 'attachments'
    assert call_args.user == test_user
    assert call_args.all_mailboxes is True

@pytest.mark.asyncio
async def test_main_sync_missing_user(mock_argv, capsys, mock_config):
    mock_argv(['sync', 'headers']) 
    with pytest.raises(SystemExit) as e:
        await main_module.main()
    assert e.type == SystemExit
    assert e.value.code == 2
    captured = capsys.readouterr()
    assert "error: the following arguments are required for sync: --user" in captured.err

@pytest.mark.skip(reason="Skipping due to ModuleNotFoundError: No module named 'mcp' in fastmcp_server.py")
@pytest.mark.asyncio
@patch('main.uvicorn.run') 
@patch('fastmcp_server.mcp', new_callable=MagicMock) 
async def test_main_dispatches_serve_mcp(mock_fastmcp_mcp_source, mock_uvicorn_run, mock_argv, mock_config):
    test_host = "127.0.0.1"
    test_port = 9999
    mock_argv([
        'serve-mcp',
        '--mcp-host', test_host,
        '--mcp-port', str(test_port)
    ])
    await main_module.main()
    mock_uvicorn_run.assert_called_once_with(
        mock_fastmcp_mcp_source, 
        host=test_host,
        port=test_port,
        log_level="info"
    )

# --- Tests for Handler Functions ---

@pytest.mark.asyncio
@patch('main.display_mailboxes', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.get_credentials')
async def test_handle_list_mailboxes_success(
    mock_get_creds, MockImapClientParam, mock_display_mailboxes, mock_config, mocked_imap_client_interface
):
    args = MagicMock()
    args.creds = "fake_creds.json"; args.host = "imap.example.com"; args.user = "user@example.com"
    mock_creds_obj = MagicMock(name="MockCredsObject")
    mock_get_creds.return_value = mock_creds_obj
    
    # Configure MockImapClientParam (the patch object for main.ImapClient) to return our shared mock
    MockImapClientParam.return_value = mocked_imap_client_interface 
    
    await main_module.handle_list_mailboxes_command(args)
    
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    # Assert calls on the shared mock instance
    mocked_imap_client_interface.connect.assert_called_once()
    mock_display_mailboxes.assert_called_once_with(mocked_imap_client_interface)
    mocked_imap_client_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sys.exit') 
@patch('main.ImapClient') # Will use mocked_imap_client_interface
@patch('main.get_credentials')
async def test_handle_list_mailboxes_creds_failure(
    mock_get_creds, MockImapClientParam, mock_sys_exit, mock_config, mocked_imap_client_interface
):
    args = MagicMock()
    args.creds = "bad_creds.json"
    mock_get_creds.side_effect = FileNotFoundError("Credentials not found")
    
    # Even if get_credentials fails, ImapClient might not be instantiated.
    # If it were, we'd set it up like:
    # MockImapClientParam.return_value = mocked_imap_client_interface

    with pytest.raises(FileNotFoundError, match="Credentials not found"):
        await main_module.handle_list_mailboxes_command(args)
    
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_not_called() # ImapClient should not be instantiated if creds fail
    # mock_sys_exit.assert_called_once_with(1) # sys.exit is not called if FileNotFoundError propagates

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock)
@patch('main.execute_query', new_callable=AsyncMock) 
@patch('main.DatabaseManager') # Will use mocked_db_interface
async def test_handle_query_list_queries(
    MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config, mocked_db_interface
):
    args = MagicMock()
    args.db = "test.db"; args.list_queries = True; args.query_name = None
    
    MockDbManagerParam.return_value = mocked_db_interface
    
    await main_module.handle_query_command(args)
    
    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once()
    mock_list_queries.assert_called_once_with(mocked_db_interface)
    mock_execute_query.assert_not_called()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock) 
@patch('main.execute_query', new_callable=AsyncMock)
@patch('main.DatabaseManager') # Will use mocked_db_interface
async def test_handle_query_execute_specific_query(
    MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config, mocked_db_interface
):
    args = MagicMock()
    args.db = "query.db"; args.query_name = "my_query"; args.list_queries = False
    args.limit = 10; args.start_date = None; args.end_date = None; args.output_format = 'json'
    args.message_id = None # Explicitly set message_id to None for this test
    
    MockDbManagerParam.return_value = mocked_db_interface
    
    await main_module.handle_query_command(args)
    
    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once()
    mock_execute_query.assert_called_once_with(
        mocked_db_interface, 
        args.query_name, 
        limit=args.limit
        # output_format, start_date, end_date, message_id will use defaults in execute_query
    )
    mock_list_queries.assert_not_called()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock)
@patch('main.execute_query', new_callable=AsyncMock) 
@patch('main.DatabaseManager') # Will use mocked_db_interface
@patch('builtins.print') 
async def test_handle_query_no_name_and_not_list(
    mock_print, MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config, mocked_db_interface
):
    args = MagicMock()
    args.db = "query.db"; args.query_name = None; args.list_queries = False

    MockDbManagerParam.return_value = mocked_db_interface

    await main_module.handle_query_command(args)

    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once() # Connects before realizing no action
    mock_print.assert_any_call("Error: No query name specified. Use --list-queries to see available queries.")
    mock_list_queries.assert_called_once_with(mocked_db_interface)
    mock_execute_query.assert_not_called()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.run_analytics', new_callable=AsyncMock)
@patch('main.DatabaseManager') # Will use mocked_db_interface
async def test_handle_analytics_command_calls_run_analytics(
    MockDbManagerParam, mock_run_analytics, mock_config, mocked_db_interface
):
    args = MagicMock()
    args.db = "analytics.db"; args.year = 2023; args.calendar = True; args.metric = 'size'
    
    MockDbManagerParam.return_value = mocked_db_interface
    
    await main_module.handle_analytics_command(args)
    
    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once()
    mock_run_analytics.assert_called_once_with(
        mocked_db_interface,
        year=args.year, 
        calendar=args.calendar, 
        metric=args.metric
    )
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sys.exit')
@patch('main.get_credentials')
@patch('main.DatabaseManager') # Will use mocked_db_interface
@patch('main.ImapClient')      # Will use mocked_imap_client_interface
async def test_handle_sync_creds_failure(
    MockImapClientParam, MockDbManagerParam, mock_get_creds, mock_sys_exit, mock_config,
    mocked_db_interface, mocked_imap_client_interface # Add shared fixtures
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'headers'; args.db = "sync.db"; args.creds = "bad_creds.json"
    args.user = "user"; args.host = "host"; args.mailbox = "INBOX"; args.all_mailboxes = False
    
    mock_get_creds.side_effect = FileNotFoundError("Credentials not found for sync")
    MockDbManagerParam.return_value = mocked_db_interface # DB Manager is still created
    # ImapClient might not be instantiated if creds fail early

    with pytest.raises(FileNotFoundError, match="Credentials not found for sync"):
        await main_module.handle_sync_command(args)

    # MockDbManagerParam.assert_called_once_with(args.db) # DBManager is not called if creds fail
    MockDbManagerParam.assert_not_called()
    # mocked_db_interface.connect.assert_called_once() # Not called
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_not_called()
    # mocked_db_interface.close.assert_called_once() # Not called

@pytest.mark.asyncio
@patch('main.sync_email_headers', new_callable=AsyncMock)
@patch('main.ImapClient')      # Use mocked_imap_client_interface
@patch('main.DatabaseManager') # Use mocked_db_interface
@patch('main.get_credentials')
async def test_handle_sync_headers_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_headers_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'headers'; args.db = "sync_h.db"; args.creds = "creds.json"
    args.user = "hdr_user"; args.host = "hdr_host"; args.mailbox = "MyHeaders"; args.all_mailboxes = False
    
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    MockImapClientParam.return_value = mocked_imap_client_interface
    mocked_imap_client_interface.list_mailboxes.return_value = ["MyHeaders", "INBOX"]

    await main_module.handle_sync_command(args)

    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once()
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    mocked_imap_client_interface.connect.assert_called_once() # From ImapClient instantiation
    
    mock_sync_headers_func.assert_called_once_with(
        db_manager=mocked_db_interface, 
        imap_client=mocked_imap_client_interface, 
        mailbox=args.mailbox
    )
    mocked_imap_client_interface.close.assert_called_once()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_email_headers', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_headers_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_headers_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'headers'; args.db = "sync_all_h.db"; args.creds = "creds.json"
    args.user = "all_hdr_user"; args.host = "all_hdr_host"; args.mailbox = "INBOX" # Default, but overridden by all_mailboxes
    args.all_mailboxes = True
    
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    MockImapClientParam.return_value = mocked_imap_client_interface
    
    mailboxes_to_sync = ["BOX1", "BOX2", "[Gmail]/Spam"]
    mocked_imap_client_interface.list_mailboxes.return_value = mailboxes_to_sync

    await main_module.handle_sync_command(args)

    MockDbManagerParam.assert_called_once_with(args.db)
    mocked_db_interface.connect.assert_called_once()
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    mocked_imap_client_interface.connect.assert_called_once() 
    mocked_imap_client_interface.list_mailboxes.assert_called_once()

    assert mock_sync_headers_func.call_count == len(mailboxes_to_sync)
    for mbox in mailboxes_to_sync:
        mock_sync_headers_func.assert_any_call(
            db_manager=mocked_db_interface, 
            imap_client=mocked_imap_client_interface, 
            mailbox=mbox
        )
    
    mocked_imap_client_interface.close.assert_called_once()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_full_emails', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_full_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_full_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'full'; args.db = "sync_f.db"; args.creds = "creds.json"
    args.user = "full_user"; args.host = "full_host"; args.mailbox = "Archive"; args.all_mailboxes = False

    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    MockImapClientParam.return_value = mocked_imap_client_interface
    mocked_imap_client_interface.list_mailboxes.return_value = ["Archive", "INBOX"]

    await main_module.handle_sync_command(args)

    # Similar assertions as for headers, but calling sync_full_emails
    mock_sync_full_func.assert_called_once_with(
        db_manager=mocked_db_interface, 
        imap_client=mocked_imap_client_interface, 
        mailbox=args.mailbox
    )
    mocked_imap_client_interface.close.assert_called_once()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_full_emails', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_full_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_full_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'full'; args.db = "sync_all_f.db"; args.creds = "creds.json"
    args.user = "all_full_user"; args.host = "all_full_host"; args.mailbox = "SENT"; args.all_mailboxes = True

    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    MockImapClientParam.return_value = mocked_imap_client_interface
    mailboxes_to_sync = ["Sent", "Drafts"]
    mocked_imap_client_interface.list_mailboxes.return_value = mailboxes_to_sync

    await main_module.handle_sync_command(args)
    
    assert mock_sync_full_func.call_count == len(mailboxes_to_sync)
    for mbox in mailboxes_to_sync:
        mock_sync_full_func.assert_any_call(
            db_manager=mocked_db_interface, 
            imap_client=mocked_imap_client_interface, 
            mailbox=mbox
        )
    mocked_imap_client_interface.close.assert_called_once()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_attachments', new_callable=AsyncMock)
# Attachments sync might not always need ImapClient, but handler creates it if creds are there
@patch('main.ImapClient') 
@patch('main.DatabaseManager')
@patch('main.get_credentials') 
async def test_handle_sync_attachments_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_attachments_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'attachments'; args.db = "sync_a.db"; args.creds = "creds.json"
    args.user = "att_user"; args.host = "att_host"; args.mailbox = "Trash"; args.all_mailboxes = False

    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    MockImapClientParam.return_value = mocked_imap_client_interface # IMAP client is created
    mocked_imap_client_interface.list_mailboxes.return_value = ["Trash", "INBOX"]

    await main_module.handle_sync_command(args)

    mock_sync_attachments_func.assert_called_once_with(
        db_manager=mocked_db_interface, 
        # imap_client is not passed to sync_attachments
        mailbox=args.mailbox
    )
    # IMAP client is created by handler, so it should be connected and closed
    # --- Correction: IMAP client is NOT created or connected for 'attachments' mode ---
    # mocked_imap_client_interface.connect.assert_called_once()
    mocked_db_interface.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_attachments', new_callable=AsyncMock)
@patch('main.ImapClient') 
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_attachments_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_attachments_func, mock_config,
    mocked_db_interface, mocked_imap_client_interface # mocked_imap_client_interface is not used here
):
    args = MagicMock()
    args.command = 'sync'; args.sync_mode = 'attachments'; args.db = "sync_all_a.db"; args.creds = "creds.json"
    args.user = "all_att_user"; args.host = "all_att_host"; args.mailbox = "INBOX"; args.all_mailboxes = True

    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    MockDbManagerParam.return_value = mocked_db_interface
    # MockImapClientParam.return_value = mocked_imap_client_interface # IMAP client not used for this path
    
    mailboxes_from_db = ["All Mail", "Important"]
    # Mock the call to db_manager.get_mailboxes_from_full_emails()
    mocked_db_interface.get_mailboxes_from_full_emails = AsyncMock(return_value=mailboxes_from_db)

    await main_module.handle_sync_command(args)

    mocked_db_interface.get_mailboxes_from_full_emails.assert_called_once()
    assert mock_sync_attachments_func.call_count == len(mailboxes_from_db)
    for mbox in mailboxes_from_db:
        mock_sync_attachments_func.assert_any_call(
            db_manager=mocked_db_interface, 
            mailbox=mbox
        )
    # IMAP client is not connected or used for listing mailboxes in this mode
    # mocked_imap_client_interface.connect.assert_called_once()
    # mocked_imap_client_interface.list_mailboxes.assert_called_once()
    mocked_db_interface.close.assert_called_once()