import argparse
import asyncio
import datetime
import sys
from typing import List, Dict, Any

# Local imports
import config # Ensure config is imported to allow modification of DEBUG_MODE
from config import DEFAULT_DB_PATH, DEFAULT_CREDS_JSON_PATH
from db import DatabaseManager
from imap_client import ImapClient, get_credentials, display_mailboxes
from sync import sync_email_headers, sync_full_emails, sync_attachments
from queries import METRIC_QUERIES, execute_query, list_available_queries
from analytics import run_analytics

# Placeholder for handler functions to be defined later
async def handle_list_mailboxes_command(args):
    # This function will handle the logic for listing mailboxes
    # It will need to initialize ImapClient and DatabaseManager as appropriate
    print(f"Listing mailboxes with args: {args}") # Placeholder
    creds = get_credentials(args.creds) 
    if not creds:
        sys.exit("Failed to get credentials for listing mailboxes.")
    
    imap_client = None
    try:
        imap_client = ImapClient(args.host, args.user, creds)
        await imap_client.connect()
        await display_mailboxes(imap_client)
    finally:
        if imap_client:
            await imap_client.close()

async def handle_query_command(args):
    print(f"Executing query with args: {args}") # Placeholder
    db_manager = DatabaseManager(args.db)
    await db_manager.connect()
    try:
        if args.list_queries:
            await list_available_queries()
        elif args.query_name:
            query_params = {}
            if args.limit is not None: query_params['limit'] = args.limit
            if args.start_date: query_params['start_date'] = args.start_date
            if args.end_date: query_params['end_date'] = args.end_date
            if args.message_id: query_params['message_id'] = args.message_id
            await execute_query(db_manager.db, args.query_name, **query_params)
        else:
            print("Error: No query name specified. Use --list-queries to see available queries.")
            await list_available_queries()
    finally:
        if db_manager and db_manager.db:
            await db_manager.close()

async def handle_analytics_command(args):
    print(f"Running analytics with args: {args}") # Placeholder
    db_manager = DatabaseManager(args.db)
    await db_manager.connect()
    try:
        await run_analytics(db_manager, args)
    finally:
        if db_manager and db_manager.db:
            await db_manager.close()

async def handle_sync_command(args):
    print(f"Running sync ({args.sync_mode}) with args: {args}") # Placeholder
    creds = get_credentials(args.creds)
    if not creds:
        sys.exit(f"Failed to get credentials for sync mode {args.sync_mode}.")

    db_manager = DatabaseManager(args.db)
    await db_manager.connect()
    
    imap_client = None
    try:
        if args.sync_mode in ['headers', 'full']:
            imap_client = ImapClient(args.host, args.user, creds)
            await imap_client.connect()

        sync_target_mailbox = args.mailbox
        all_mailboxes_flag = args.all_mailboxes

        if args.sync_mode == 'headers':
            if all_mailboxes_flag:
                mailboxes_to_sync = await imap_client.list_mailboxes()
                for mbx in mailboxes_to_sync:
                    await sync_email_headers(db_manager, imap_client, mbx)
            else:
                await sync_email_headers(db_manager, imap_client, sync_target_mailbox)
        elif args.sync_mode == 'full':
            if all_mailboxes_flag:
                mailboxes_to_sync = await imap_client.list_mailboxes()
                for mbx in mailboxes_to_sync:
                    await sync_full_emails(db_manager, imap_client, mbx)
            else:
                await sync_full_emails(db_manager, imap_client, sync_target_mailbox)
        elif args.sync_mode == 'attachments':
            # Attachments mode might not need a live imap client if working from DB
            if all_mailboxes_flag:
                mailboxes_to_sync = await db_manager.get_mailboxes_from_full_emails()
                for mbx in mailboxes_to_sync:
                    await sync_attachments(db_manager, mbx)
            else:
                await sync_attachments(db_manager, sync_target_mailbox)
        print(f"Sync mode '{args.sync_mode}' completed.")
    finally:
        if imap_client:
            await imap_client.close()
        if db_manager and db_manager.db:
            await db_manager.close()

async def main():
    parser = argparse.ArgumentParser(description='Email Management and Analytics Tool')
    parser.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to SQLite database file.')
    parser.add_argument('--creds', default=DEFAULT_CREDS_JSON_PATH, help='Path to OAuth2 client secrets JSON (e.g., client_secret.json).')
    parser.add_argument('--host', default='imap.gmail.com', help='IMAP host address.')
    parser.add_argument('--user', help='Your Gmail address for IMAP authentication.')
    parser.add_argument('--debug', action='store_true', help='Enable detailed debug output.')

    subparsers = parser.add_subparsers(title='commands', dest='command', required=True, help='Available commands')

    # --- List Mailboxes Command ---
    list_parser = subparsers.add_parser('list-mailboxes', help='List all available mailboxes on the server.')
    # list-mailboxes typically only needs --host, --user, --creds (from main parser)

    # --- Query Command ---
    query_parser = subparsers.add_parser('query', help='Execute predefined SQL queries against the email database.')
    query_parser.add_argument('--list-queries', action='store_true', help='List all available predefined queries.')
    query_parser.add_argument('query_name', nargs='?', help='Name of the query to execute (omit if using --list-queries).')
    query_parser.add_argument('--limit', type=int, help='Limit the number of results for the query.')
    query_parser.add_argument('--start-date', help='Start date for date-sensitive queries (YYYY-MM-DD).')
    query_parser.add_argument('--end-date', help='End date for date-sensitive queries (YYYY-MM-DD).')
    query_parser.add_argument('--message-id', help='Message-ID for email thread queries.')

    # --- Analytics Command ---
    analytics_parser = subparsers.add_parser('analytics', help='Generate and display analytics from the email database.')
    analytics_parser.add_argument('--year', type=int, default=datetime.datetime.now().year, help='Year for which to generate analytics (default: current year).')
    analytics_parser.add_argument('--calendar', action='store_true', help='Display a calendar heatmap instead of a monthly density chart.')
    analytics_parser.add_argument('--metric', choices=list(METRIC_QUERIES.keys()), default='emails', help='Metric to visualize (e.g., emails, attachments). Default: emails.')

    # --- Sync Commands (as a group with sub-sub-commands or modes) ---
    sync_parser = subparsers.add_parser('sync', help='Synchronize emails with the local database.')
    sync_subparsers = sync_parser.add_subparsers(title='sync_modes', dest='sync_mode', required=True, help='Specific sync operation')

    common_sync_args_parser = argparse.ArgumentParser(add_help=False) # Parent for common sync args
    common_sync_args_parser.add_argument('--mailbox', default='INBOX', help='Mailbox to target for sync (default: INBOX).')
    common_sync_args_parser.add_argument('--all-mailboxes', action='store_true', help='Apply sync operation to all accessible mailboxes.')

    headers_parser = sync_subparsers.add_parser('headers', help='Sync only email headers.', parents=[common_sync_args_parser])
    full_parser = sync_subparsers.add_parser('full', help='Sync full email content (requires headers to be synced first).' , parents=[common_sync_args_parser])
    attachments_parser = sync_subparsers.add_parser('attachments', help='Extract and normalize attachments from synced full emails.', parents=[common_sync_args_parser])
    
    # --- Serve MCP Command ---
    serve_mcp_parser = subparsers.add_parser('serve-mcp', help='Start the Model Context Protocol (MCP) server.')
    serve_mcp_parser.add_argument('--mcp-host', default='0.0.0.0', help='Host for the MCP server (default: 0.0.0.0).')
    serve_mcp_parser.add_argument('--mcp-port', type=int, default=8001, help='Port for the MCP server (default: 8001).')
    # Add other MCP server specific configurations here if needed, e.g., path to specific Ollama models if not global

    args = parser.parse_args()

    if args.debug:
        config.DEBUG_MODE = True # Set DEBUG_MODE in the config module
        print("Debug mode enabled (via config.DEBUG_MODE).")
        print(f"Parsed arguments: {args}")

    # Command dispatching
    if args.command == 'list-mailboxes':
        # Ensure user is provided for list-mailboxes if not already enforced by ImapClient/get_credentials
        if not args.user:
            parser.error("the following arguments are required for list-mailboxes: --user")
        await handle_list_mailboxes_command(args)
    elif args.command == 'query':
        await handle_query_command(args)
    elif args.command == 'analytics':
        await handle_analytics_command(args)
    elif args.command == 'sync':
        # Ensure user is provided for sync modes if not already enforced
        if not args.user:
            parser.error("the following arguments are required for sync: --user")
        await handle_sync_command(args)
    elif args.command == 'serve-mcp':
        # Import uvicorn and the mcp app instance here to avoid circular dependencies
        # or making uvicorn a top-level import if only used for this command.
        import uvicorn
        from fastmcp_server import mcp as mcp_application # Import the app instance
        print(f"Starting MCP Server on {args.mcp_host}:{args.mcp_port}")
        uvicorn.run(mcp_application, host=args.mcp_host, port=args.mcp_port, log_level="info")
    else:
        parser.print_help()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
    except Exception as e:
        import traceback
        print(f"\nProgram terminated due to an unhandled error: {e}")
        print(traceback.format_exc())
        sys.exit(1)
