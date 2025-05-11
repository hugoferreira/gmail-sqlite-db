import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
import sys

from db import DatabaseManager as ActualDatabaseManager
from imap_client import ImapClient as ActualImapClient
import main as main_module
import datetime # Ensure datetime is imported for tests that might use it indirectly or for patching

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
async def test_main_dispatches_analytics_default(mock_handler, mock_argv, mock_config):
    # Command first, its args after. If global args were used, they'd be before 'analytics'
    mock_argv(['analytics'])
    await main_module.main()
    mock_handler.assert_called_once()
    call_args = mock_handler.call_args[0][0]
    assert call_args.command == 'analytics'
    assert call_args.db == main_module.DEFAULT_DB_PATH
    assert isinstance(call_args.year, int)
    assert call_args.calendar is False
    assert call_args.metric == 'emails'

@pytest.mark.asyncio
@patch('main.handle_analytics_command', new_callable=AsyncMock)
@patch('main.datetime') 
async def test_main_dispatches_analytics_custom(mock_main_datetime, mock_handler, mock_argv, mock_config):
    mock_main_datetime.datetime.now.return_value.year = 2023 
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
    mock_get_creds, MockImapClientParam, mock_display_mailboxes, mock_config
):
    args = MagicMock()
    args.creds = "fake_creds.json"; args.host = "imap.example.com"; args.user = "user@example.com"
    mock_creds_obj = MagicMock(name="MockCredsObject")
    mock_get_creds.return_value = mock_creds_obj
    mock_imap_instance = AsyncMock(spec=ActualImapClient) 
    MockImapClientParam.return_value = mock_imap_instance 
    await main_module.handle_list_mailboxes_command(args)
    mock_get_creds.assert_called_once_with(args.creds)
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    mock_imap_instance.connect.assert_called_once()
    mock_display_mailboxes.assert_called_once_with(mock_imap_instance)
    mock_imap_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sys.exit') 
@patch('main.get_credentials')
async def test_handle_list_mailboxes_creds_failure(
    mock_get_creds, mock_sys_exit, mock_config
):
    args = MagicMock()
    args.creds = "bad_creds.json"
    mock_get_creds.return_value = None
    mock_sys_exit.side_effect = SystemExit 
    with pytest.raises(SystemExit):
       await main_module.handle_list_mailboxes_command(args)
    mock_get_creds.assert_called_once_with(args.creds)
    mock_sys_exit.assert_called_once_with("Failed to get credentials for listing mailboxes.")

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock)
@patch('main.execute_query', new_callable=AsyncMock) 
@patch('main.DatabaseManager')
async def test_handle_query_list_queries(
    MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config
):
    args = MagicMock()
    args.db = "test.db"; args.list_queries = True; args.query_name = None
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager) 
    mock_db_instance.db = AsyncMock() 
    MockDbManagerParam.return_value = mock_db_instance
    await main_module.handle_query_command(args)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    mock_list_queries.assert_called_once()
    mock_execute_query.assert_not_called()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock) 
@patch('main.execute_query', new_callable=AsyncMock)
@patch('main.DatabaseManager')
async def test_handle_query_execute_specific_query(
    MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config
):
    args = MagicMock()
    args.db = "mydb.sqlite"; args.list_queries = False; args.query_name = "my_query"
    args.limit = 50; args.start_date = "2023-05-01"; args.end_date = None; args.message_id = "<id>"
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager) 
    mock_db_instance.db = AsyncMock(name="MockAiosqliteConnection")
    MockDbManagerParam.return_value = mock_db_instance
    expected_params = {'limit': 50, 'start_date': "2023-05-01", 'message_id': "<id>"}
    await main_module.handle_query_command(args)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    mock_execute_query.assert_called_once_with(mock_db_instance.db, args.query_name, **expected_params)
    mock_list_queries.assert_not_called()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.list_available_queries', new_callable=AsyncMock)
@patch('main.execute_query', new_callable=AsyncMock) 
@patch('main.DatabaseManager')
@patch('builtins.print') 
async def test_handle_query_no_name_and_not_list(
    mock_print, MockDbManagerParam, mock_execute_query, mock_list_queries, mock_config
):
    args = MagicMock()
    args.db = "nodb.db"; args.list_queries = False; args.query_name = None
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager) 
    mock_db_instance.db = AsyncMock()
    MockDbManagerParam.return_value = mock_db_instance
    await main_module.handle_query_command(args)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    mock_print.assert_any_call("Error: No query name specified. Use --list-queries to see available queries.")
    mock_list_queries.assert_called_once() 
    mock_execute_query.assert_not_called()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.run_analytics', new_callable=AsyncMock)
@patch('main.DatabaseManager')
async def test_handle_analytics_command_calls_run_analytics(
    MockDbManagerParam, mock_run_analytics, mock_config
):
    args = MagicMock()
    args.db = "analytics_test.db"; args.year = 2024; args.calendar = True; args.metric = "attachments"
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager) 
    mock_db_instance.db = AsyncMock() 
    MockDbManagerParam.return_value = mock_db_instance
    await main_module.handle_analytics_command(args)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    mock_run_analytics.assert_called_once_with(mock_db_instance, args)
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sys.exit')
@patch('main.get_credentials')
@patch('main.DatabaseManager') 
async def test_handle_sync_creds_failure(
    MockDbManagerParam, mock_get_creds, mock_sys_exit, mock_config
):
    args = MagicMock()
    args.creds = "super_bad_creds.json"; args.sync_mode = "headers"; args.db = "sync_db_fail.db"
    mock_get_creds.return_value = None
    mock_sys_exit.side_effect = SystemExit 
    with pytest.raises(SystemExit):
        await main_module.handle_sync_command(args)
    mock_get_creds.assert_called_once_with(args.creds)
    MockDbManagerParam.assert_not_called() # Should not be called if exit happens before
    mock_sys_exit.assert_called_once_with(f"Failed to get credentials for sync mode {args.sync_mode}.")
    # db_manager instance and its methods (connect, close) should not be called

@pytest.mark.asyncio
@patch('main.sync_email_headers', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_headers_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_headers_func, mock_config
):
    args = MagicMock()
    args.db="h_sync.db"; args.creds="c.json"; args.host="h.com"; args.user="u"
    args.sync_mode="headers"; args.mailbox="INBOX_TEST"; args.all_mailboxes=False
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock() # Ensure .db attribute exists for the finally block check
    MockDbManagerParam.return_value = mock_db_instance
    mock_imap_instance = AsyncMock(spec=ActualImapClient)
    MockImapClientParam.return_value = mock_imap_instance
    await main_module.handle_sync_command(args)
    mock_get_creds.assert_called_once_with(args.creds)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    mock_imap_instance.connect.assert_called_once()
    mock_sync_headers_func.assert_called_once_with(mock_db_instance, mock_imap_instance, args.mailbox)
    mock_imap_instance.close.assert_called_once()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_email_headers', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_headers_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_headers_func, mock_config
):
    args = MagicMock()
    args.db="h_all.db"; args.creds="ca.json"; args.host="ha.com"; args.user="ua"
    args.sync_mode="headers"; args.mailbox="IGNORE"; args.all_mailboxes=True
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock()
    MockDbManagerParam.return_value = mock_db_instance
    mock_imap_instance = AsyncMock(spec=ActualImapClient)
    mailboxes_to_sync = ["INBOX", "Sent"]
    mock_imap_instance.list_mailboxes = AsyncMock(return_value=mailboxes_to_sync)
    MockImapClientParam.return_value = mock_imap_instance
    await main_module.handle_sync_command(args)
    mock_get_creds.assert_called_once_with(args.creds)
    MockDbManagerParam.assert_called_once_with(args.db)
    mock_db_instance.connect.assert_called_once()
    MockImapClientParam.assert_called_once_with(args.host, args.user, mock_creds_obj)
    mock_imap_instance.connect.assert_called_once()
    mock_imap_instance.list_mailboxes.assert_called_once()
    assert mock_sync_headers_func.call_count == len(mailboxes_to_sync)
    for mbx in mailboxes_to_sync:
        mock_sync_headers_func.assert_any_call(mock_db_instance, mock_imap_instance, mbx)
    mock_imap_instance.close.assert_called_once()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_full_emails', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_full_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_full_func, mock_config
):
    args = MagicMock()
    args.db="f_sync.db"; args.creds="fc.json"; args.host="fh.com"; args.user="fu"
    args.sync_mode="full"; args.mailbox="SENT"; args.all_mailboxes=False
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock()
    MockDbManagerParam.return_value = mock_db_instance
    mock_imap_instance = AsyncMock(spec=ActualImapClient)
    MockImapClientParam.return_value = mock_imap_instance
    await main_module.handle_sync_command(args)
    mock_sync_full_func.assert_called_once_with(mock_db_instance, mock_imap_instance, args.mailbox)
    mock_imap_instance.close.assert_called_once()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_full_emails', new_callable=AsyncMock)
@patch('main.ImapClient')
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_full_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_full_func, mock_config
):
    args = MagicMock()
    args.db="fa.db"; args.creds="fca.json"; args.host="fha.com"; args.user="fua"
    args.sync_mode="full"; args.all_mailboxes=True
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock()
    MockDbManagerParam.return_value = mock_db_instance
    mock_imap_instance = AsyncMock(spec=ActualImapClient)
    mailboxes_to_sync = ["All Mail", "Spam"]
    mock_imap_instance.list_mailboxes = AsyncMock(return_value=mailboxes_to_sync)
    MockImapClientParam.return_value = mock_imap_instance
    await main_module.handle_sync_command(args)
    assert mock_sync_full_func.call_count == len(mailboxes_to_sync)
    for mbx in mailboxes_to_sync:
        mock_sync_full_func.assert_any_call(mock_db_instance, mock_imap_instance, mbx)
    mock_imap_instance.close.assert_called_once()
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_attachments', new_callable=AsyncMock)
@patch('main.ImapClient') 
@patch('main.DatabaseManager')
@patch('main.get_credentials') 
async def test_handle_sync_attachments_single_mailbox(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_attachments_func, mock_config
):
    args = MagicMock()
    args.db="a_sync.db"; args.creds="ac.json"; args.host="ah.com"; args.user="au"
    args.sync_mode="attachments"; args.mailbox="TARGET"; args.all_mailboxes=False
    mock_creds_obj = MagicMock() 
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock()
    MockDbManagerParam.return_value = mock_db_instance
    await main_module.handle_sync_command(args)
    MockImapClientParam.assert_not_called() 
    mock_sync_attachments_func.assert_called_once_with(mock_db_instance, args.mailbox)
    mock_db_instance.close.assert_called_once()

@pytest.mark.asyncio
@patch('main.sync_attachments', new_callable=AsyncMock)
@patch('main.ImapClient') 
@patch('main.DatabaseManager')
@patch('main.get_credentials')
async def test_handle_sync_attachments_all_mailboxes(
    mock_get_creds, MockDbManagerParam, MockImapClientParam, mock_sync_attachments_func, mock_config
):
    args = MagicMock()
    args.db="aa.db"; args.creds="aca.json"; args.sync_mode="attachments"; args.all_mailboxes=True
    mock_creds_obj = MagicMock()
    mock_get_creds.return_value = mock_creds_obj
    mock_db_instance = AsyncMock(spec=ActualDatabaseManager)
    mock_db_instance.db = MagicMock()
    mailboxes_from_db = ["DB1", "DB2"]
    mock_db_instance.get_mailboxes_from_full_emails = AsyncMock(return_value=mailboxes_from_db)
    MockDbManagerParam.return_value = mock_db_instance
    await main_module.handle_sync_command(args)
    MockImapClientParam.assert_not_called()
    mock_db_instance.get_mailboxes_from_full_emails.assert_called_once()
    assert mock_sync_attachments_func.call_count == len(mailboxes_from_db)
    for mbx in mailboxes_from_db:
        mock_sync_attachments_func.assert_any_call(mock_db_instance, mbx)
    mock_db_instance.close.assert_called_once()

# End of tests for handle_sync_command 